#include <chrono>
#include <thread>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <tlx/cmdline_parser.hpp>

#include "RandomObjectProvider.h"

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
size_t iterations = 1;

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

template<typename ObjectStore, typename IoManager>
void performQueries(ObjectStore &objectStore, ObjectProvider &objectProvider, std::vector<uint64_t> &keys) {
    std::vector<VariableSizeObjectStore::QueryHandle> queryHandles;
    queryHandles.resize(queueDepth);
    for (int i = 0; i < queueDepth; i++) {
        queryHandles.at(i).buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, objectStore.requiredBufferPerQuery()));
    }
    ObjectStoreView<ObjectStore, IoManager> objectStoreView(objectStore, useCachedIo ? 0 : (O_DIRECT | O_SYNC), queueDepth);

    auto queryStart = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < queueDepth; i++) {
        queryHandles.at(i).key = keys.at(rand() % numObjects);
        objectStoreView.submitSingleQuery(&queryHandles.at(i));
    }
    objectStoreView.submit();
    size_t queriesDone = queueDepth;
    while (queriesDone < numQueries) {
        VariableSizeObjectStore::QueryHandle *queryHandle = objectStoreView.awaitAny();
        while (queryHandle != nullptr) {
            validateValue(queryHandle, objectProvider);
            queryHandle->key = keys.at(rand() % numObjects);
            objectStoreView.submitSingleQuery(queryHandle);
            queriesDone++;
            queryHandle = objectStoreView.peekAny();
        }
        objectStoreView.submit();
        objectStore.LOG("Querying", queriesDone/128, numQueries/128);
    }
    for (size_t i = 0; i < queueDepth; i++) {
        VariableSizeObjectStore::QueryHandle *queryHandle = objectStoreView.awaitAny();
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
             << " io=" << objectStoreView.ioManager.name()
             << " spaceUsage=" << objectStore.internalSpaceUsage()
             << timerAverage
             << " queriesPerSecond=" << queriesPerSecond
             << std::endl;
}

template<typename ObjectStore, typename IoManager>
void runTest() {
    std::vector<uint64_t> keys = generateRandomKeys(numObjects);
    RandomObjectProvider objectProvider(lengthDistribution, averageObjectSize);

    ObjectStore objectStore(fillDegree, storeFile.c_str());

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
                  << " spaceUsage=" << objectStore.internalSpaceUsage()
                  << objectStore.constructionTimer
                  << std::endl;
        return;
    }

    objectStore.LOG("Letting CPU cool down");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    objectStore.LOG("Querying");
    performQueries<ObjectStore, IoManager>(objectStore, objectProvider, keys);
    std::cout<<std::endl;
}

template <typename ObjectStore>
void dispatchIoManager() {

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

template<template<size_t _> class ObjectStore>
void dispatchObjectStore(size_t param, IntList<>) {
    std::cerr<<"The parameter "<<param<<" for "<<typeid(ObjectStore<0>).name()<<" was not compiled into this binary."<<std::endl;
}
template <template<size_t _> class ObjectStore, size_t I, size_t ...ListRest>
void dispatchObjectStore(size_t param, IntList<I, ListRest...>) {
    if (I != param) {
        return dispatchObjectStore<ObjectStore, ListRest...>(param, IntList<ListRest...>());
    } else {
        dispatchIoManager<ObjectStore<I>>();
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
    cmd.add_size_t('i', "iterations", iterations, "Perform the same benchmark multiple times.");

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

    for (size_t i = 0; i < iterations; i++) {
        if (efParameterA != 0) {
            dispatchObjectStore<EliasFanoObjectStore>(efParameterA, IntList<2, 4, 8, 16, 32, 128>());
        }
        if (separatorBits != 0) {
            dispatchObjectStore<SeparatorObjectStore>(separatorBits, IntList<4, 5, 6, 8, 10>());
        }
        if (cuckoo) {
            dispatchIoManager<ParallelCuckooObjectStore>();
        }
    }
    return 0;
}