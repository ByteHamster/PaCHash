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

static std::vector<uint64_t> randomKeys(size_t N) {
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

void setToRandomKeys(QueryHandle &handle, std::vector<uint64_t> &keys) {
    for (int i = 0; i < handle.keys.size(); i++) {
        handle.keys.at(i) = keys.at(rand() % keys.size());
    }
}

void validateValuesNotNullptr(QueryHandle &handle) {
    for (int i = 0; i < handle.keys.size(); i++) {
        if (handle.resultPointers.at(i) == nullptr) {
            std::cerr<<"Error: Returned value is null"<<std::endl;
            exit(1);
        }
    }
}

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
        setToRandomKeys(queryHandle, keys);
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
    queryHandle.stats.print();
    std::cout<<"Total query time: "<<std::chrono::duration_cast<std::chrono::milliseconds>(time5 - time4).count() << " ms"<<std::endl;
}

template<class IoManager = MemoryMapIO<>>
static void testVariableSizeObjectStores(size_t numObjects, float fillDegree, size_t numBatches, size_t numQueriesPerBatch) {
    std::vector<uint64_t> keys = randomKeys(numObjects);
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

template<class IoManager = MemoryMapIO<>>
void testMultipleQueryHandles(size_t numObjects, int numQueryHandles, int numBatches, int numQueriesPerBatch) {
    std::vector<uint64_t> keys = randomKeys(numObjects);
    const char* filename = "key_value_store.txt";
    EliasFanoObjectStore<8, VerboseBenchmarkConfig<IoManager>> objectStore(filename);
    objectStore.writeToFile(keys, objectProvider);
    objectStore.reloadFromFile();

    objectStore.LOG("Syncing filesystem before query");
    system("sync");
    objectStore.LOG("Querying");

    std::vector<QueryHandle> queryHandles;
    for (int i = 0; i < numQueryHandles; i++) {
        queryHandles.push_back(objectStore.newQueryHandle(numQueriesPerBatch));
    }
    int currentQueryHandle = 0; // Round-robin

    auto queryStart = std::chrono::high_resolution_clock::now();
    for (int batch = 0; batch < numBatches; batch++) {
        if (!queryHandles.at(currentQueryHandle).completed) {
            // Ignore this on first run
            objectStore.awaitCompletion(queryHandles.at(currentQueryHandle));
            validateValuesNotNullptr(queryHandles.at(currentQueryHandle));
        }
        setToRandomKeys(queryHandles.at(currentQueryHandle), keys);
        objectStore.submitQuery(queryHandles.at(currentQueryHandle));
        currentQueryHandle = (currentQueryHandle + 1) % numQueryHandles;
    }
    auto queryEnd = std::chrono::high_resolution_clock::now();
    int totalQueries = numBatches * numQueriesPerBatch;
    long time = std::chrono::duration_cast<std::chrono::nanoseconds>(queryEnd - queryStart).count();
    std::cout<<"\rQueries per second: "<< std::round(((double)totalQueries/time)*1000*1000)
            << " (" << time/totalQueries << " ns/query)" <<std::endl;
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

    /*int numQueriesPerBatch = 10;
    testMultipleQueryHandles<UringIO<fileFlags>>(N, 1, numBatches, numQueriesPerBatch);
    testMultipleQueryHandles<UringIO<fileFlags>>(N, 5, numBatches, numQueriesPerBatch);
    testMultipleQueryHandles<UringIO<fileFlags>>(N, 10, numBatches, numQueriesPerBatch);
    testMultipleQueryHandles<UringIO<fileFlags>>(N, 20, numBatches, numQueriesPerBatch);
    testMultipleQueryHandles<UringIO<fileFlags>>(N, 30, numBatches, numQueriesPerBatch);*/
    return 0;
}
