#include <chrono>
#include <thread>
#include <exception>
#include <IoManager.h>
#include <PactHashObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <BumpingHashObjectStore.h>
#include <tlx/cmdline_parser.hpp>

#include "RandomObjectProvider.h"
#include "Barrier.h"

#define SEED_RANDOM (~0ul)
size_t numObjects = 1e6;
double fillDegree = 0.96;
size_t averageObjectSize = 244;
std::string lengthDistribution = RandomObjectProvider::getDistributions().at(0).first;
size_t numQueries = 5e3;
size_t queueDepth = 128;
bool usePosixIo = false, usePosixAio = false, useUringIo = false, useIoSubmit = false;
bool useCachedIo = false;
bool verifyResults = false;
std::string storeFile;
size_t pactHashParameterA = 0;
size_t separatorBits = 0;
bool cuckoo = false;
bool bumpingHash = false;
bool readOnly = false;
size_t keyGenerationSeed = SEED_RANDOM;
size_t iterations = 1;
size_t numThreads = 1;
std::mutex queryOutputMutex;
std::unique_ptr<Barrier> queryOutputBarrier = nullptr;
RandomObjectProvider randomObjectProvider;

struct BenchmarkSettings {
    friend auto operator<<(std::ostream& os, BenchmarkSettings const& q) -> std::ostream& {
        (void) q;
        os << " numQueries=" << numQueries
           << " queueDepth=" << queueDepth
           << " blockSize=" << pacthash::StoreConfig::BLOCK_LENGTH
           << " numObjects=" << numObjects
           << " fillDegree=" << fillDegree
           << " threads=" << numThreads
           << " objectSize=" << averageObjectSize
           << " objectSizeDistribution=" << lengthDistribution;
        return os;
    }
};

static std::vector<pacthash::StoreConfig::key_t> generateRandomKeys(size_t N) {
    unsigned int seed = std::random_device{}();
    if (keyGenerationSeed != SEED_RANDOM) {
         seed = keyGenerationSeed;
    }
    std::cout<<"# Seed for input keys: "<<seed<<std::endl;
    pacthash::XorShift64 generator(seed);
    std::vector<pacthash::StoreConfig::key_t> keys;
    keys.reserve(N);
    for (size_t i = 0; i < N; i++) {
        pacthash::StoreConfig::key_t key = generator();
        keys.emplace_back(key);
    }
    return keys;
}

inline void validateValue(pacthash::VariableSizeObjectStore::QueryHandle *handle) {
    if (handle->resultPtr == nullptr) {
        throw std::logic_error("Returned value is null for key " + std::to_string(handle->key));
    }
    if (verifyResults) {
        if (handle->length != randomObjectProvider.getLength(handle->key)) {
            throw std::logic_error("Returned length is wrong for key " + std::to_string(handle->key)
                    + ", expected " + std::to_string(randomObjectProvider.getLength(handle->key))
                    + " but got " + std::to_string(handle->length));
        }
        int delta = memcmp(handle->resultPtr, randomObjectProvider.getValue(handle->key), handle->length);
        if (delta != 0) {
            throw std::logic_error("Unexpected result for key " + std::to_string(handle->key)
                   + ", expected " + std::string(randomObjectProvider.getValue(handle->key), handle->length)
                   + " but got " + std::string(handle->resultPtr, handle->length));
        }
    }
}

template<typename ObjectStore, typename IoManager>
void performQueries(ObjectStore &objectStore, std::vector<pacthash::StoreConfig::key_t> &keys) {
    std::vector<pacthash::VariableSizeObjectStore::QueryHandle> queryHandles;
    queryHandles.resize(queueDepth);
    for (size_t i = 0; i < queueDepth; i++) {
        queryHandles.at(i).buffer = new (std::align_val_t(pacthash::StoreConfig::BLOCK_LENGTH)) char[objectStore.requiredBufferPerQuery()];
    }
    pacthash::ObjectStoreView<ObjectStore, IoManager> objectStoreView(objectStore, useCachedIo ? 0 : O_DIRECT, queueDepth);

    pacthash::XorShift64 prng(time(nullptr));
    auto queryStart = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < queueDepth; i++) {
        queryHandles[i].key = keys[prng(numObjects)];
        objectStoreView.enqueueQuery(&queryHandles[i]);
    }
    objectStoreView.submit();
    size_t queriesDone = queueDepth;
    size_t batches = 1;
    while (queriesDone < numQueries) {
        pacthash::VariableSizeObjectStore::QueryHandle *queryHandle = objectStoreView.awaitAny();
        while (queryHandle != nullptr) {
            validateValue(queryHandle);
            queryHandle->key = keys[prng(numObjects)];
            objectStoreView.enqueueQuery(queryHandle);
            queriesDone++;
            queryHandle = objectStoreView.peekAny();
        }
        batches++;
        objectStoreView.submit();
        objectStore.LOG("Querying", queriesDone/32, numQueries/32);
    }
    for (size_t i = 0; i < queueDepth; i++) {
        pacthash::VariableSizeObjectStore::QueryHandle *queryHandle = objectStoreView.awaitAny();
        validateValue(queryHandle);
    }
    auto queryEnd = std::chrono::high_resolution_clock::now();

    long timeMicroseconds = std::chrono::duration_cast<std::chrono::microseconds>(queryEnd - queryStart).count();
    std::cout<<"\rExecuted "<<numQueries<<" queries in "<<timeMicroseconds/1000<<" ms, "
            <<(double)queriesDone/(double)batches<<" queries/batch"<<std::endl;
    double queriesPerMicrosecond = (double)numQueries/(double)timeMicroseconds;
    double queriesPerSecond = 1000.0 * 1000.0 * queriesPerMicrosecond;

    pacthash::QueryTimer timerAverage;
    for (auto & queryHandle : queryHandles) {
        timerAverage += queryHandle.stats;
        delete[] queryHandle.buffer;
    }
    timerAverage /= queryHandles.size();

    queryOutputBarrier->wait(); // Wait until all are done querying
    std::unique_lock<std::mutex> lock(queryOutputMutex); // Print one by one

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
    std::vector<pacthash::StoreConfig::key_t> keys = generateRandomKeys(numObjects);

    ObjectStore objectStore(fillDegree, storeFile.c_str(), useCachedIo ? 0 : O_DIRECT);

    std::cout<<"# "<<ObjectStore::name()<<" in "<<storeFile<<" with N="<<numObjects<<", alpha="<<fillDegree<<std::endl;
    if (!readOnly) {
        auto HashFunction = [](const pacthash::StoreConfig::key_t &key) -> pacthash::StoreConfig::key_t {
            return key;
        };
        auto LengthEx = [](const pacthash::StoreConfig::key_t &key) -> pacthash::StoreConfig::length_t {
            return randomObjectProvider.getLength(key);
        };
        auto ValueEx = [](const pacthash::StoreConfig::key_t &key) -> const char * {
            return randomObjectProvider.getValue(key);
        };
        objectStore.writeToFile(keys.begin(), keys.end(), HashFunction, LengthEx, ValueEx);
        objectStore.LOG("Syncing written file");
        sync();
    }
    objectStore.reloadFromFile();

    if (numQueries == 0) {
        objectStore.printConstructionStats();
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
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    if (numThreads == 1) {
        performQueries<ObjectStore, IoManager>(objectStore, keys);
    } else {
        for (size_t thread = 0; thread < numThreads; thread++) {
            threads.emplace_back([&] {
                performQueries<ObjectStore, IoManager>(objectStore, keys);
            });
        }
        for (size_t thread = 0; thread < numThreads; thread++) {
            threads.at(thread).join();
        }
    }
    std::cout<<std::endl;
}

template <typename ObjectStore>
void dispatchIoManager() {

    if (usePosixIo) {
        runTest<ObjectStore, pacthash::PosixIO>();
    }
    if (usePosixAio) {
        #ifdef HAS_LIBAIO
            runTest<ObjectStore, pacthash::PosixAIO>();
        #else
            throw std::runtime_error("Requested Posix AIO but compiled without it.");
        #endif
    }
    if (useUringIo) {
        #ifdef HAS_LIBURING
            runTest<ObjectStore, pacthash::UringIO>();
        #else
            throw std::runtime_error("Requested Uring IO but compiled without it.");
        #endif
    }
    if (useIoSubmit) {
        runTest<ObjectStore, pacthash::LinuxIoSubmit>();
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
    cmd.add_bytes('n', "num_objects", numObjects, "Number of objects in the data store, supports SI units (eg. 10M)");
    cmd.add_double('d', "fill_degree", fillDegree, "Fill degree on the external storage. Elias-Fano method always uses 1.0");
    cmd.add_bytes('o', "object_size", averageObjectSize, "Average object size. Disk stores the size plus a table entry of size " + std::to_string(pacthash::VariableSizeObjectStore::overheadPerObject));
    cmd.add_string('l', "object_size_distribution", lengthDistribution, "Distribution of the object lengths. Values: " + RandomObjectProvider::getDistributionsString());
    cmd.add_string('f', "store_file", storeFile, "File to store the external-memory data structures in.");
    cmd.add_bool('y', "read_only", readOnly, "Don't write the file and assume that there already is a valid file. "
              "Undefined behavior if the file is not valid or was created with another method. Only makes sense in combination with --key_seed.");
    cmd.add_size_t('x', "key_seed", keyGenerationSeed, "Seed for the key generation. When not specified, uses a random seed for each run.");
    cmd.add_size_t('t', "num_threads", numThreads, "Number of threads to execute the benchmark in.");

    cmd.add_bytes('q', "num_queries", numQueries, "Number of keys to query, supports SI units (eg. 10M)");
    cmd.add_size_t('p', "queue_depth", queueDepth, "Number of queries to keep in flight");
    cmd.add_bool('v', "verify", verifyResults, "Check if the result returned from the data structure matches the expected result");
    cmd.add_size_t('i', "iterations", iterations, "Perform the same benchmark multiple times.");

    cmd.add_size_t('e', "pacthash", pactHashParameterA, "Run the PactHash method with the given number of bins per page");
    cmd.add_size_t('s', "separator", separatorBits, "Run the separator method with the given number of separator bits");
    cmd.add_bool('c', "cuckoo", cuckoo, "Run the cuckoo method");
    cmd.add_bool('b', "bumping", bumpingHash, "Run the bumping hash table");

    cmd.add_bool('r', "posix_io", usePosixIo , "Include Posix (read()) file IO benchmarks");
    cmd.add_bool('a', "posix_aio", usePosixAio , "Include Posix AIO benchmarks");
    cmd.add_bool('u', "uring_io", useUringIo , "Include Linux Uring IO benchmarks");
    cmd.add_bool('i', "io_submit", useIoSubmit , "Include Linux io_submit syscall for IO benchmarks");
    cmd.add_bool('c', "cached_io", useCachedIo , "Allow the system to cache IO in RAM instead of using O_DIRECT");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    if (!cuckoo && !bumpingHash && separatorBits == 0 && pactHashParameterA == 0) {
        std::cerr<<"No method specified"<<std::endl;
        cmd.print_usage();
        return 1;
    } else if (!usePosixIo && !usePosixAio && !useUringIo && !useIoSubmit) {
        std::cerr<<"No IO method specified"<<std::endl;
        cmd.print_usage();
        return 1;
    } else if (fillDegree > 1 || fillDegree <= 0) {
        std::cerr<<"Fill degree needs to be between 0 and 1"<<std::endl;
        return 1;
    }

    randomObjectProvider = RandomObjectProvider(lengthDistribution, numObjects, averageObjectSize);
    queryOutputBarrier = std::make_unique<Barrier>(numThreads);
    for (size_t i = 0; i < iterations; i++) {
        if (pactHashParameterA != 0) {
            dispatchObjectStore<pacthash::PactHashObjectStore>(pactHashParameterA, IntList<2, 4, 8, 16, 32, 128>());
        }
        if (separatorBits != 0) {
            dispatchObjectStore<pacthash::SeparatorObjectStore>(separatorBits, IntList<4, 5, 6, 8, 10>());
        }
        if (cuckoo) {
            dispatchIoManager<pacthash::ParallelCuckooObjectStore>();
        }
        if (bumpingHash) {
            dispatchIoManager<pacthash::BumpingHashObjectStore>();
        }
    }
    return 0;
}