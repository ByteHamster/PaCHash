#include <IoManager.h>
#include <string>
#include <iostream>
#include <chrono>
#include <EliasFanoObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>

#include "RandomObjectProvider.h"

RandomObjectProvider<NORMAL_DISTRIBUTION, 256 - sizeof(ObjectHeader)> objectProvider;

template <typename IoManager_>
struct VerboseBenchmarkConfig : public VariableSizeObjectStoreConfig {
    public:
        static constexpr bool SHOW_PROGRESS = true;
        static constexpr int PROGRESS_STEPS = 20;
        using IoManager = IoManager_;
};

template <class T>
void performTest(T &objectStore, size_t numBatches, size_t numQueriesPerBatch, std::vector<uint64_t> &keys) {
    auto time1 = std::chrono::high_resolution_clock::now();
    objectStore.writeToFile(keys, objectProvider);
    auto time2 = std::chrono::high_resolution_clock::now();
    objectStore.reloadFromFile();
    auto time3 = std::chrono::high_resolution_clock::now();
    objectStore.printConstructionStats();

    std::cout<<"Construction duration: "
        <<std::chrono::duration_cast<std::chrono::milliseconds>(time2 - time1).count() << " ms writing, "
        <<std::chrono::duration_cast<std::chrono::milliseconds>(time3 - time2).count() << " ms reloading"<<std::endl;

    T::LOG("Syncing filesystem before query");
    system("sync");
    QueryHandle queryHandle = objectStore.newQueryHandle(numQueriesPerBatch);
    auto time4 = std::chrono::high_resolution_clock::now();
    for (int q = 0; q < numBatches; q++) {
        for (int i = 0; i < numQueriesPerBatch; i++) {
            queryHandle.keys.at(i) = keys.at(rand() % objectStore.numObjects);
        }
        objectStore.submitQuery(queryHandle);
        objectStore.awaitCompletion(queryHandle);

        for (int i = 0; i < queryHandle.keys.size(); i++) {
            uint64_t key = queryHandle.keys.at(i);
            size_t length = queryHandle.resultLengths.at(i);
            char *valuePointer = queryHandle.resultPointers.at(i);
            if (valuePointer == nullptr) {
                std::cerr<<"Object not found"<<std::endl;
                exit(1);
            }
            std::string got(valuePointer, length);
            std::string expected(objectProvider.getValue(key), objectProvider.getLength(key));
            if (expected != got) {
                std::cerr<<"Unexpected result for key "<<key<<", expected "<<expected<<" but got "<<got<<std::endl;
                exit(1);
            }
            T::LOG("Querying", q, numBatches);
        }
    }
    auto time5 = std::chrono::high_resolution_clock::now();
    std::cout << "\r";
    objectStore.printQueryStats();
    std::cout<<"Total query time: "<<std::chrono::duration_cast<std::chrono::milliseconds>(time5 - time4).count() << " ms"<<std::endl;
}

template<class IoManager = MemoryMapIO<>>
static void testVariableSizeObjectStores(size_t numObjects, float fillDegree, size_t numBatches, size_t numQueriesPerBatch) {
    std::cout<<"# Generating input keys"<<std::flush;
    std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    std::vector<uint64_t> keys;
    keys.reserve(numObjects);
    for (size_t i = 0; i < numObjects; i++) {
        uint64_t key = dist(generator);
        keys.emplace_back(key);
    }
    std::cout << "\r"<<std::flush;

    const char* filename = "key_value_store.txt";
    {
        EliasFanoObjectStore<8, VerboseBenchmarkConfig<IoManager>> eliasFanoStore(filename);
        performTest(eliasFanoStore, numBatches, numQueriesPerBatch, keys);
        std::cout << std::endl;
    }
    {
        SeparatorObjectStore<6, VerboseBenchmarkConfig<IoManager>> separatorHashingStore(fillDegree, filename);
        performTest(separatorHashingStore, numBatches, numQueriesPerBatch, keys);
        std::cout << std::endl;
    }
    {
        ParallelCuckooObjectStore<VerboseBenchmarkConfig<IoManager>> cuckooHashing(fillDegree, filename);
        performTest(cuckooHashing, numBatches, numQueriesPerBatch, keys);
        std::cout<<std::endl;
    }
}

int main() {
    constexpr int fileFlags = O_DIRECT | O_SYNC;
    constexpr int N = 1e6;
    constexpr float fillDegree = 0.98;
    int numBatches = 1e6;

    testVariableSizeObjectStores<MemoryMapIO<fileFlags>>(N, fillDegree, numBatches, 1);

    numBatches /= 3000; // Hard disks with O_DIRECT are slow
    testVariableSizeObjectStores<PosixIO<fileFlags>>(N, fillDegree, numBatches, 1);
    testVariableSizeObjectStores<PosixAIO<fileFlags>>(N, fillDegree, numBatches, 1);
    testVariableSizeObjectStores<UringIO<fileFlags>>(N, fillDegree, numBatches, 1);
    testVariableSizeObjectStores<LinuxIoSubmit<fileFlags>>(N, fillDegree, numBatches, 1);

    numBatches /= 10; // Multiple queries per batch
    testVariableSizeObjectStores<PosixIO<fileFlags>>(N, fillDegree, numBatches, 10);
    testVariableSizeObjectStores<PosixAIO<fileFlags>>(N, fillDegree, numBatches, 10);
    testVariableSizeObjectStores<UringIO<fileFlags>>(N, fillDegree, numBatches, 10);
    testVariableSizeObjectStores<LinuxIoSubmit<fileFlags>>(N, fillDegree, numBatches, 10);
    return 0;
}
