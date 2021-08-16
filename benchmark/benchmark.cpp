#include <IoManager.h>
#include <string>
#include <iostream>
#include <chrono>
#include <EliasFanoObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <tlx/cmdline_parser.hpp>

#include "RandomObjectProvider.h"

RandomObjectProvider<NORMAL_DISTRIBUTION, 256 - sizeof(ObjectHeader)> objectProvider;
size_t numObjects = 1e6;
double fillDegree = 0.98;
size_t numBatches = 2e4;
size_t numParallelBatches = 1;
size_t batchSize = 10;
bool useMmapIo = false, usePosixIo = true, usePosixAio = false, useUringIo = false, useIoSubmit = false;
bool useCachedIo = false;
bool verifyResults = false;
std::vector<std::string> storeFiles;
size_t efParameterA = 0;
size_t separatorBits = 0;
bool cuckoo = false;

static std::vector<uint64_t> generateRandomKeys(size_t N) {
    std::cout<<"# Generating input keys"<<std::flush;
    std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    std::vector<uint64_t> keys;
    keys.reserve(N);
    for (size_t i = 0; i < N; i++) {
        uint64_t key = dist(generator);
        keys.emplace_back(key);
    }
    std::cout << "\r"<<std::flush;
    return keys;
}

void setToRandomKeys(VariableSizeObjectStore::QueryHandle &handle, std::vector<uint64_t> &keys) {
    for (uint64_t &key : handle.keys) {
        key = keys.at(rand() % keys.size());
    }
}

template <bool nullcheckOnly>
void validateValues(VariableSizeObjectStore::QueryHandle &handle) {
    for (int i = 0; i < handle.keys.size(); i++) {
        uint64_t key = handle.keys.at(i);
        size_t length = handle.resultLengths.at(i);
        char *valuePointer = handle.resultPointers.at(i);

        if (valuePointer == nullptr) {
            std::cerr<<"Error: Returned value is null"<<std::endl;
            exit(1);
        }
        if constexpr (!nullcheckOnly) {
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
void runTest() {
    std::vector<uint64_t> keys = generateRandomKeys(numObjects);

    ObjectStore objectStore1(fillDegree, "key_value_store.txt");
    auto time1 = std::chrono::high_resolution_clock::now();
    objectStore1.writeToFile(keys, objectProvider);
    auto time2 = std::chrono::high_resolution_clock::now();
    objectStore1.reloadFromFile();
    auto time3 = std::chrono::high_resolution_clock::now();
    objectStore1.printConstructionStats();
    std::cout<<"Construction duration: "
             <<std::chrono::duration_cast<std::chrono::milliseconds>(time2 - time1).count() << " ms writing, "
             <<std::chrono::duration_cast<std::chrono::milliseconds>(time3 - time2).count() << " ms reloading"<<std::endl;

    ObjectStore objectStore2(fillDegree,"key_value_store2.txt");
    objectStore2.writeToFile(keys, objectProvider);
    objectStore2.reloadFromFile();

    objectStore1.LOG("Syncing filesystem before query");
    system("sync");

    std::vector<VariableSizeObjectStore::QueryHandle> queryHandles;
    queryHandles.reserve(numParallelBatches);
    for (int i = 0; i < numParallelBatches; i++) {
        if ((i % 2) == 0) {
            queryHandles.push_back(objectStore1.template newQueryHandle<IoManager>(numParallelBatches, useCachedIo ? 0 : O_DIRECT | O_SYNC));
        } else {
            queryHandles.push_back(objectStore2.template newQueryHandle<IoManager>(numParallelBatches, useCachedIo ? 0 : O_DIRECT | O_SYNC));
        }
    }
    int currentQueryHandle = 0; // Round-robin

    auto queryStart = std::chrono::high_resolution_clock::now();
    for (int batch = 0; batch < numBatches; batch++) {
        if (!queryHandles.at(currentQueryHandle).completed) {
            // Ignore this on first run
            queryHandles.at(currentQueryHandle).awaitCompletion();
            validateValues<true>(queryHandles.at(currentQueryHandle));
        }
        setToRandomKeys(queryHandles.at(currentQueryHandle), keys);
        queryHandles.at(currentQueryHandle).submit();
        currentQueryHandle = (currentQueryHandle + 1) % numParallelBatches;
        objectStore1.LOG("Querying", batch, numBatches);
    }
    auto queryEnd = std::chrono::high_resolution_clock::now();
    int totalQueries = numBatches * numParallelBatches;
    long time = std::chrono::duration_cast<std::chrono::nanoseconds>(queryEnd - queryStart).count();
    std::cout<<"\rPerformance: "<< std::round(((double)totalQueries/time)*1000*1000)
            << " kQueries/s (" << time/totalQueries << " ns/query)" <<std::endl;
    queryHandles.at(0).stats.print();
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
        runTest<ObjectStore, PosixAIO>();
    }
    if (useUringIo) {
        runTest<ObjectStore, UringIO>();
    }
    if (useIoSubmit) {
        runTest<ObjectStore, LinuxIoSubmit>();
    }
}

void dispatchObjectStore() {
    if (efParameterA != 0) {
        switch (efParameterA) {
            case 4:
                dispatchIoManager<EliasFanoObjectStore<4>>();
                break;
            case 8:
                dispatchIoManager<EliasFanoObjectStore<8>>();
                break;
            case 16:
                dispatchIoManager<EliasFanoObjectStore<16>>();
                break;
            default:
                std::cerr<<"Selected Elias-Fano parameter was not compiled into this binary. Available: 4, 8, 16"<<std::endl;
                break;
        }
    }
    if (separatorBits != 0) {
        switch (separatorBits) {
            case 4:
                dispatchIoManager<SeparatorObjectStore<4>>();
                break;
            case 6:
                dispatchIoManager<SeparatorObjectStore<6>>();
                break;
            case 8:
                dispatchIoManager<SeparatorObjectStore<8>>();
                break;
            default:
                std::cerr<<"Selected separator bits were not compiled into this binary. Available: 4, 6, 8"<<std::endl;
        }
    }
    if (cuckoo) {
        dispatchIoManager<ParallelCuckooObjectStore>();
    }
}

int main(int argc, char** argv) {
    storeFiles.emplace_back("key_value_store.txt");

    tlx::CmdlineParser cmd;
    cmd.add_size_t('n', "num_objects", numObjects, "Number of objects in the data store");
    cmd.add_double('d', "fill_degree", fillDegree, "Fill degree on the external storage. Elias-Fano method always uses 1.0");
    cmd.add_size_t('b', "num_batches", numBatches, "Number of query batches to execute");
    cmd.add_size_t('p', "num_parallel_batches", numBatches, "Number of parallel query batches to execute");
    cmd.add_size_t('b', "batch_size", batchSize, "Number of keys to query per batch");
    cmd.add_bool('v', "verify", verifyResults, "Check if the result returned from the data structure matches the expected result");
    cmd.add_stringlist('f', "store_files", storeFiles, "Files to store the external-memory data structures in");

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
    } else if (storeFiles.empty()) {
        std::cerr<<"No output files specified"<<std::endl;
        cmd.print_usage();
        return 1;
    } else if (numParallelBatches % storeFiles.size() != 0) {
        std::cerr<<"Number of parallel batches must be a multiple of the number of store files"<<std::endl;
        return 1;
    } else if (!cuckoo && separatorBits == 0 && efParameterA == 0) {
        std::cerr<<"No method specified"<<std::endl;
        cmd.print_usage();
        return 1;
    }

    dispatchObjectStore();
    return 0;
}