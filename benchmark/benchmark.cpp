#include <chrono>
#include <thread>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <tlx/cmdline_parser.hpp>

#include "RandomObjectProvider.h"
#include "Barrier.h"

#define SEED_RANDOM (-1)
size_t numObjects = 1e6;
double fillDegree = 0.96;
size_t averageObjectSize = 244;
int lengthDistribution = NORMAL_DISTRIBUTION;
size_t numQueries = 5e3;
size_t queueDepth = 32;
bool usePosixIo = false, usePosixAio = false, useUringIo = false, useIoSubmit = false;
bool useCachedIo = false;
bool verifyResults = false;
std::string storeFile;
size_t efParameterA = 0;
size_t separatorBits = 0;
bool cuckoo = false;
bool readOnly = false;
size_t keyGenerationSeed = SEED_RANDOM;

struct BenchmarkSettings {
    friend auto operator<<(std::ostream& os, BenchmarkSettings const& q) -> std::ostream& {
        os << " numQueries=" << numQueries
           << " queueDepth=" << queueDepth
           << " numObjects=" << numObjects
           << " fillDegree=" << fillDegree
           << " objectSize=" << averageObjectSize;
        return os;
    }
};

static std::vector<uint64_t> generateRandomKeys(size_t N) {
    uint64_t seed = std::random_device{}();
    if (keyGenerationSeed != SEED_RANDOM) {
         seed = keyGenerationSeed;
    }
    std::cout<<"# Seed for input keys: "<<seed<<std::endl;
    std::mt19937_64 generator(seed);
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    std::vector<uint64_t> keys;
    keys.reserve(N);
    for (size_t i = 0; i < N; i++) {
        uint64_t key = dist(generator);
        keys.emplace_back(key);
    }
    return keys;
}

inline void validateValue(VariableSizeObjectStore::QueryHandle *handle, ObjectProvider &objectProvider) {
    if (handle->resultPtr == nullptr) {
        std::cerr<<"Error: Returned value is null for key "<<handle->key<<std::endl;
        assert(false);
        exit(1);
    }
    if (verifyResults) {
        std::string got(handle->resultPtr, handle->length);
        std::string expected(objectProvider.getValue(handle->key), objectProvider.getLength(handle->key));
        if (expected != got) {
            std::cerr<<"Unexpected result for key "<<handle->key<<", expected "<<expected<<" but got "<<got<<std::endl;
            assert(false);
            exit(1);
        }
    }
}

template<typename ObjectStore>
void performQueries(ObjectStore &objectStore, ObjectProvider &objectProvider, std::vector<uint64_t> &keys) {
    std::vector<VariableSizeObjectStore::QueryHandle> queryHandles;
    queryHandles.resize(queueDepth);
    for (int i = 0; i < queueDepth; i++) {
        queryHandles.at(i).buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, objectStore.requiredBufferPerQuery()));
    }

    auto queryStart = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < queueDepth; i++) {
        queryHandles.at(i).key = keys.at(rand() % numObjects);
        objectStore.submitQuery(&queryHandles.at(i));
    }
    for (size_t i = queueDepth; i < numQueries; i++) {
        VariableSizeObjectStore::QueryHandle *queryHandle = objectStore.awaitAny();
        validateValue(queryHandle, objectProvider);
        queryHandle->key = keys.at(rand() % numObjects);
        objectStore.submitQuery(queryHandle);
        objectStore.LOG("Querying", i, numQueries);
    }
    for (size_t i = 0; i < queueDepth; i++) {
        VariableSizeObjectStore::QueryHandle *queryHandle = objectStore.awaitAny();
        validateValue(queryHandle, objectProvider);
    }
    auto queryEnd = std::chrono::high_resolution_clock::now();

    long timeMicroseconds = std::chrono::duration_cast<std::chrono::microseconds>(queryEnd - queryStart).count();
    std::cout<<"\rExecuted "<<numQueries<<" queries in "<<timeMicroseconds/1000<<" ms"<<std::endl;
    double queriesPerMicrosecond = (double)numQueries/(double)timeMicroseconds;
    double queriesPerSecond = 1000.0 * 1000.0 * queriesPerMicrosecond;
    objectStore.printQueryStats();

    QueryTimer timerAverage;
    for (auto & queryHandle : queryHandles) {
        timerAverage += queryHandle.stats;
        free(queryHandle.buffer);
    }
    timerAverage /= queryHandles.size();

    std::cout<<"RESULT"
             << BenchmarkSettings()
             << " method=" << ObjectStore::name()
             << " io=" << objectStore.ioManager->name()
             << " spaceUsage=" << objectStore.internalSpaceUsage()
             << timerAverage
             << " queriesPerSecond=" << queriesPerSecond
             << std::endl;
}

template<typename ObjectStore>
void runTest(IoManager *ioManager) {
    std::vector<uint64_t> keys = generateRandomKeys(numObjects);
    RandomObjectProvider objectProvider(lengthDistribution, averageObjectSize);

    ObjectStore objectStore(fillDegree, storeFile.c_str());
    objectStore.ioManager = ioManager;

    std::cout<<"# "<<ObjectStore::name()<<" in "<<storeFile<<" with N="<<numObjects<<", alpha="<<fillDegree<<std::endl;
    auto time1 = std::chrono::high_resolution_clock::now();
    if (!readOnly) {
        objectStore.writeToFile(keys, objectProvider);
        objectStore.LOG("Syncing written file");
        int result = system("sync");
        if (result != 0) {
            std::cerr<<"Unable to sync file system"<<std::endl;
        }
    }
    objectStore.reloadFromFile();

    if (numQueries == 0) {
        std::cout<<std::endl;
        std::cout << "RESULT"
                  << BenchmarkSettings()
                  << " method=" << ObjectStore::name()
                  << " io=" << ioManager->name()
                  << " spaceUsage=" << objectStore.internalSpaceUsage()
                  << objectStore.constructionTimer
                  << std::endl;
        return;
    }

    objectStore.LOG("Letting CPU cool down");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    performQueries<ObjectStore>(objectStore, objectProvider, keys);
    std::cout<<std::endl;
}

template <typename ObjectStore>
void dispatchIoManager() {
    int oflags = useCachedIo ? 0 : (O_DIRECT | O_SYNC);

    if (usePosixIo) {
        runTest<ObjectStore>(new PosixIO(storeFile.c_str(), oflags, queueDepth));
    }
    if (usePosixAio) {
        #ifdef HAS_LIBAIO
            runTest<ObjectStore>(new PosixAIO(storeFile.c_str(), oflags, queueDepth));
        #else
            std::cerr<<"Requested Posix AIO but compiled without it."<<std::endl;
            exit(1);
        #endif
    }
    if (useUringIo) {
        #ifdef HAS_LIBURING
            runTest<ObjectStore>(new UringIO(storeFile.c_str(), oflags, queueDepth));
        #else
            std::cerr<<"Requested Uring IO but compiled without it."<<std::endl;
            exit(1);
        #endif
    }
    if (useIoSubmit) {
        runTest<ObjectStore>(new LinuxIoSubmit(storeFile.c_str(), oflags, queueDepth));
    }
}

template <size_t ...> struct IntList {};

void dispatchObjectStoreEliasFano(IntList<>) {
    std::cerr<<"The requested Elias-Fano parameter was not compiled into this binary."<<std::endl;
}
template <size_t I, size_t ...ListRest>
void dispatchObjectStoreEliasFano(IntList<I, ListRest...>) {
    if (I != efParameterA) {
        return dispatchObjectStoreEliasFano(IntList<ListRest...>());
    } else {
        dispatchIoManager<EliasFanoObjectStore<I>>();
    }
}

void dispatchObjectStoreSeparator(IntList<>) {
    std::cerr<<"The requested separator bits parameter was not compiled into this binary."<<std::endl;
}
template <size_t I, size_t ...ListRest>
void dispatchObjectStoreSeparator(IntList<I, ListRest...>) {
    if (I != separatorBits) {
        return dispatchObjectStoreSeparator(IntList<ListRest...>());
    } else {
        dispatchIoManager<SeparatorObjectStore<I>>();
    }
}

int main(int argc, char** argv) {
    storeFile = "key_value_store.txt";

    tlx::CmdlineParser cmd;
    cmd.add_size_t('n', "num_objects", numObjects, "Number of objects in the data store");
    cmd.add_double('d', "fill_degree", fillDegree, "Fill degree on the external storage. Elias-Fano method always uses 1.0");
    cmd.add_size_t('o', "object_size", averageObjectSize, "Average object size. Disk stores the size plus a header of size " + std::to_string(sizeof(uint16_t) + sizeof(uint64_t)));
    cmd.add_int('l', "object_size_distribution", lengthDistribution, "Distribution of the object lengths. "
              "Normal: " + std::to_string(NORMAL_DISTRIBUTION) + ", Exponential: " + std::to_string(EXPONENTIAL_DISTRIBUTION) + ", Equal: " + std::to_string(EQUAL_DISTRIBUTION));
    cmd.add_string('f', "store_file", storeFile, "File to store the external-memory data structures in.");
    cmd.add_bool('y', "read_only", readOnly, "Don't write the file and assume that there already is a valid file. "
              "Undefined behavior if the file is not valid or was created with another method. Only makes sense in combination with --key_seed.");
    cmd.add_size_t('x', "key_seed", keyGenerationSeed, "Seed for the key generation. When not specified, uses a random seed for each run.");

    cmd.add_size_t('q', "num_queries", numQueries, "Number of keys to query");
    cmd.add_size_t('p', "queue_depth", queueDepth, "Number of queries to keep in flight");
    cmd.add_bool('v', "verify", verifyResults, "Check if the result returned from the data structure matches the expected result");

    cmd.add_size_t('e', "elias_fano", efParameterA, "Run the Elias-Fano method with the given number of bins per page");
    cmd.add_size_t('s', "separator", separatorBits, "Run the separator method with the given number of separator bits");
    cmd.add_bool('c', "cuckoo", cuckoo, "Run the cuckoo method");

    cmd.add_bool('r', "posix_io", usePosixIo , "Include Posix (read()) file IO benchmarks");
    cmd.add_bool('a', "posix_aio", usePosixAio , "Include Posix AIO benchmarks");
    cmd.add_bool('u', "uring_io", useUringIo , "Include Linux Uring IO benchmarks");
    cmd.add_bool('i', "io_submit", useIoSubmit , "Include Linux io_submit syscall for IO benchmarks");
    cmd.add_bool('c', "cache_io", useCachedIo , "Allow the system to cache IO in RAM instead of using O_DIRECT");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    if (!cuckoo && separatorBits == 0 && efParameterA == 0) {
        std::cerr<<"No method specified"<<std::endl;
        cmd.print_usage();
        return 1;
    } else if (!usePosixIo && !usePosixAio && !useUringIo && !useIoSubmit) {
        std::cerr<<"No IO method specified"<<std::endl;
        cmd.print_usage();
        return 1;
    }

    if (efParameterA != 0) {
        dispatchObjectStoreEliasFano(IntList<2, 4, 8, 16, 32, 128>());
    }
    if (separatorBits != 0) {
        dispatchObjectStoreSeparator(IntList<4, 5, 6, 8, 10>());
    }
    if (cuckoo) {
        dispatchIoManager<ParallelCuckooObjectStore>();
    }
    return 0;
}