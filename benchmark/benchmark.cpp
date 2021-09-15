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
size_t numBatches = 5e3;
size_t numParallelBatches = 1;
size_t batchSize = 10;
size_t numThreads = 1;
bool useMmapIo = false, usePosixIo = false, usePosixAio = false, useUringIo = false, useIoSubmit = false;
bool useCachedIo = false;
bool verifyResults = false;
std::vector<std::string> storeFiles;
size_t efParameterA = 0;
size_t separatorBits = 0;
bool cuckoo = false;
bool readOnly = false;
size_t keyGenerationSeed = SEED_RANDOM;
std::mutex queryOutputMutex;
std::unique_ptr<Barrier> queryOutputBarrier = nullptr;

struct BenchmarkSettings {
    friend auto operator<<(std::ostream& os, BenchmarkSettings const& q) -> std::ostream& {
        os << " batchSize=" << batchSize
           << " parallelBatches=" << numParallelBatches
           << " threads=" << numThreads
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

inline void setToRandomKeys(VariableSizeObjectStore::QueryHandle *handle, const std::vector<uint64_t> &keys) {
    for (uint64_t &key : handle->keys) {
        key = keys.at(rand() % numObjects);
    }
}

inline void validateValues(VariableSizeObjectStore::QueryHandle *handle, ObjectProvider &objectProvider) {
    for (int i = 0; i < handle->keys.size(); i++) {
        uint64_t key = handle->keys.at(i);
        size_t length = handle->resultLengths.at(i);
        char *valuePointer = handle->resultPointers.at(i);

        if (valuePointer == nullptr) {
            std::cerr<<"Error: Returned value is null for key "<<key<<std::endl;
            exit(1);
        }
        if (verifyResults) {
            std::string got(valuePointer, length);
            std::string expected(objectProvider.getValue(key), objectProvider.getLength(key));
            if (expected != got) {
                std::cerr<<"Unexpected result for key "<<key<<", expected "<<expected<<" but got "<<got<<std::endl;
                exit(1);
            }
        }
    }
}

template<typename ObjectStore, class IoManager>
void performQueries(std::vector<ObjectStore> &objectStores,
                    ObjectProvider &objectProvider,
                    std::vector<uint64_t> &keys) {
    std::vector<VariableSizeObjectStore::QueryHandle*> queryHandles;
    queryHandles.reserve(numParallelBatches);
    for (int i = 0; i < numParallelBatches; i++) {
        queryHandles.push_back(objectStores.at(i % objectStores.size())
                .template newQueryHandle<IoManager>(batchSize, useCachedIo ? 0 : O_DIRECT | O_SYNC));
    }

    auto queryStart = std::chrono::high_resolution_clock::now();
    for (int batch = 0; batch < numBatches + numParallelBatches; batch++) {
        int currentQueryHandle = batch % numParallelBatches; // round-robin
        if (batch >= numParallelBatches) {
            // Ignore this on the first runs (not started yet)
            queryHandles.at(currentQueryHandle)->awaitCompletion();
            validateValues(queryHandles.at(currentQueryHandle), objectProvider);
        }
        if (batch < numBatches) {
            // Ignore this on the last runs (don't start new ones, just collect results)
            setToRandomKeys(queryHandles.at(currentQueryHandle), keys);
            queryHandles.at(currentQueryHandle)->submit();
            objectStores.at(0).LOG("Querying", batch, numBatches);
        }
    }
    auto queryEnd = std::chrono::high_resolution_clock::now();

    queryOutputBarrier->wait(); // Wait until all are done querying
    std::unique_lock<std::mutex> lock(queryOutputMutex); // Print one by one

    size_t totalQueries = numBatches * batchSize;
    long timeMicroseconds = std::chrono::duration_cast<std::chrono::microseconds>(queryEnd - queryStart).count();
    std::cout<<"\rExecuted "<<totalQueries<<" queries in "<<timeMicroseconds/1000<<" ms"<<std::endl;
    double queriesPerMicrosecond = (double)totalQueries/timeMicroseconds;
    std::cout<<"Performance: "
            << std::round(queriesPerMicrosecond * 1000.0 * 100.0) / 100.0 << " kQueries/s ("
            << std::round((double)timeMicroseconds/totalQueries * 100.0) / 100.0 << " us/query)" <<std::endl;
    for (ObjectStore &objectStore : objectStores) {
        objectStore.printQueryStats();
    }

    for (auto & queryHandle : queryHandles) {
        std::cout<<"RESULT"
                 << BenchmarkSettings()
                 << " method=" << ObjectStore::name()
                 << " io=" << IoManager::name()
                 << " spaceUsage=" << objectStores.back().internalSpaceUsage()
                 << queryHandle->stats
                 << std::endl;
        delete queryHandle;
    }
}

template<typename ObjectStore, class IoManager>
void runTest() {
    std::vector<uint64_t> keys = generateRandomKeys(numObjects);
    RandomObjectProvider objectProvider(lengthDistribution, averageObjectSize);

    std::vector<ObjectStore> objectStores;
    objectStores.reserve(storeFiles.size());
    for (std::string &filename : storeFiles) {
        objectStores.emplace_back(fillDegree, filename.c_str());

        ObjectStore &objectStore = objectStores.back();
        std::cout<<"# "<<ObjectStore::name()<<" in "<<filename<<" with N="<<numObjects<<", alpha="<<fillDegree<<std::endl;
        auto time1 = std::chrono::high_resolution_clock::now();
        if (!readOnly) {
            objectStore.writeToFile(keys, objectProvider);
        }
        auto time2 = std::chrono::high_resolution_clock::now();
        objectStore.reloadFromFile();
        auto time3 = std::chrono::high_resolution_clock::now();
        std::cout<<"Construction duration: "
                 <<std::chrono::duration_cast<std::chrono::milliseconds>(time2 - time1).count() << " ms writing, "
                 <<std::chrono::duration_cast<std::chrono::milliseconds>(time3 - time2).count() << " ms reloading"<<std::endl;
    }

    objectStores.at(0).LOG("Syncing filesystem before query");
    int result = system("sync");
    if (result != 0) {
        std::cerr<<"Unable to sync file system"<<std::endl;
    }

    if (numBatches == 0) {
        std::cout<<std::endl;
        for (ObjectStore &objectStore : objectStores) {
            std::cout << "RESULT"
                      << BenchmarkSettings()
                      << " method=" << ObjectStore::name()
                      << " io=" << IoManager::name()
                      << " spaceUsage=" << objectStores.back().internalSpaceUsage()
                      << objectStore.constructionTimer
                      << std::endl;
        }
        return;
    }

    objectStores.at(0).LOG("Letting CPU cool down");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    if (numThreads == 1) {
        performQueries<ObjectStore, IoManager>(objectStores, objectProvider, keys);
    } else {
        for (int thread = 0; thread < numThreads; thread++) {
            threads.emplace_back([&] {
                performQueries<ObjectStore, IoManager>(objectStores, objectProvider, keys);
            });
        }

        for (int thread = 0; thread < numThreads; thread++) {
            threads.at(thread).join();
        }
    }
    std::cout<<std::endl;
}

template <typename ObjectStore>
void dispatchIoManager() {
    if (useMmapIo) {
        runTest<ObjectStore, MemoryMapIO>();
    }
    if (usePosixIo) {
        runTest<ObjectStore, PosixIO>();
    }
    if (usePosixAio) {
        #ifdef HAS_LIBAIO
            runTest<ObjectStore, PosixAIO>();
        #else
            std::cerr<<"Requested Posix AIO but compiled without it."<<std::endl;
            exit(1);
        #endif
    }
    if (useUringIo) {
        #ifdef HAS_LIBURING
            runTest<ObjectStore, UringIO>();
        #else
            std::cerr<<"Requested Uring IO but compiled without it."<<std::endl;
            exit(1);
        #endif
    }
    if (useIoSubmit) {
        runTest<ObjectStore, LinuxIoSubmit>();
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
    tlx::CmdlineParser cmd;
    cmd.add_size_t('n', "num_objects", numObjects, "Number of objects in the data store");
    cmd.add_double('d', "fill_degree", fillDegree, "Fill degree on the external storage. Elias-Fano method always uses 1.0");
    cmd.add_size_t('o', "object_size", averageObjectSize, "Average object size. Disk stores the size plus a header of size " + std::to_string(sizeof(uint16_t) + sizeof(uint64_t)));
    cmd.add_int('l', "object_size_distribution", lengthDistribution, "Distribution of the object lengths. "
              "Normal: " + std::to_string(NORMAL_DISTRIBUTION) + ", Exponential: " + std::to_string(EXPONENTIAL_DISTRIBUTION) + ", Equal: " + std::to_string(EQUAL_DISTRIBUTION));
    cmd.add_stringlist('f', "store_file", storeFiles, "Files to store the external-memory data structures in. "
              "When passing the argument multiple times, the same data structure is written to multiple files and queried round-robin.");
    cmd.add_bool('y', "read_only", readOnly, "Don't write the file and assume that there already is a valid file. "
              "Undefined behavior if the file is not valid or was created with another method. Only makes sense in combination with --key_seed.");
    cmd.add_size_t('x', "key_seed", keyGenerationSeed, "Seed for the key generation. When not specified, uses a random seed for each run.");

    cmd.add_size_t('b', "num_batches", numBatches, "Number of query batches to execute");
    cmd.add_size_t('p', "num_parallel_batches", numParallelBatches, "Number of parallel query batches to execute");
    cmd.add_size_t('b', "batch_size", batchSize, "Number of keys to query per batch");
    cmd.add_size_t('t', "num_threads", numThreads, "Number of threads to execute queries with");
    cmd.add_bool('v', "verify", verifyResults, "Check if the result returned from the data structure matches the expected result");

    cmd.add_size_t('e', "elias_fano", efParameterA, "Run the Elias-Fano method with the given number of bins per page");
    cmd.add_size_t('s', "separator", separatorBits, "Run the separator method with the given number of separator bits");
    cmd.add_bool('c', "cuckoo", cuckoo, "Run the cuckoo method");

    cmd.add_bool('m', "mmap_io", useMmapIo, "Include Memory Map IO benchmarks");
    cmd.add_bool('r', "posix_io", usePosixIo , "Include Posix (read()) file IO benchmarks");
    cmd.add_bool('a', "posix_aio", usePosixAio , "Include Posix AIO benchmarks");
    cmd.add_bool('u', "uring_io", useUringIo , "Include Linux Uring IO benchmarks");
    cmd.add_bool('i', "io_submit", useIoSubmit , "Include Linux io_submit syscall for IO benchmarks");
    cmd.add_bool('c', "cache_io", useCachedIo , "Allow the system to cache IO in RAM instead of using O_DIRECT");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    if (storeFiles.empty()) {
        storeFiles.emplace_back("key_value_store.txt");
    }

    if (numParallelBatches % storeFiles.size() != 0) {
        std::cerr<<"Number of parallel batches must be a multiple of the number of store files"<<std::endl;
        return 1;
    } else if (!cuckoo && separatorBits == 0 && efParameterA == 0) {
        std::cerr<<"No method specified"<<std::endl;
        cmd.print_usage();
        return 1;
    } else if (!useMmapIo && !usePosixIo && !usePosixAio && !useUringIo && !useIoSubmit) {
        std::cerr<<"No IO method specified"<<std::endl;
        cmd.print_usage();
        return 1;
    }

    queryOutputBarrier = std::make_unique<Barrier>(numThreads);

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