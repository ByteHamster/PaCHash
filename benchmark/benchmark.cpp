#include <IoManager.h>
#include <string>
#include <iostream>
#include <chrono>
#include <EliasFanoIndexing.h>
#include <SeparatorHashing.h>
#include <ParallelCuckooHashing.h>

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
void testConstruct(T &objectStore, std::vector<uint64_t> &keys) {
    auto time1 = std::chrono::high_resolution_clock::now();
    objectStore.writeToFile(keys, objectProvider);
    auto time2 = std::chrono::high_resolution_clock::now();
    objectStore.reloadFromFile();
    auto time3 = std::chrono::high_resolution_clock::now();
    objectStore.printConstructionStats();

    std::cout<<"Construction duration: "
        <<std::chrono::duration_cast<std::chrono::milliseconds>(time2 - time1).count() << " ms writing, "
        <<std::chrono::duration_cast<std::chrono::milliseconds>(time3 - time2).count() << " ms reloading"<<std::endl;
}

template <class T>
void testPerformQueries(T &objectStore, size_t numQueries, size_t simultaneousQueries, std::vector<uint64_t> &keys) {
    T::LOG("Syncing filesystem before query");
    system("sync");
    for (int q = 0; q < numQueries; q++) {
        std::vector<uint64_t> queryKeys;
        queryKeys.reserve(simultaneousQueries);
        for (int i = 0; i < simultaneousQueries; i++) {
            queryKeys.push_back(keys.at(rand() % objectStore.numObjects));
        }
        auto result = objectStore.query(queryKeys);
        for (int i = 0; i < queryKeys.size(); i++) {
            uint64_t key = queryKeys.at(i);
            const auto& [length, valuePtr] = result.at(i);
            if (valuePtr == nullptr) {
                std::cerr<<"Object not found"<<std::endl;
            }
            std::string got(valuePtr, length);
            std::string expected(objectProvider.getValue(key), objectProvider.getLength(key));
            if (expected != got) {
                std::cerr<<"Unexpected result for key "<<key<<", expected "<<expected<<" but got "<<got<<std::endl;
            }
            T::LOG("Querying", q, numQueries);
        }
    }
    std::cout << "\r";
    objectStore.printQueryStats();
}

template<class IoManager = MemoryMapIO<>>
static void testVariableSizeObjectStores(size_t numObjects, float fillDegree, size_t simultaneousQueries) {
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

    size_t numQueries = 1e7 / simultaneousQueries;
    const char* filename = "key_value_store.txt";
    {
        EliasFanoIndexing<8, VerboseBenchmarkConfig<IoManager>> eliasFanoStore(filename);
        testConstruct(eliasFanoStore, keys);
        testPerformQueries(eliasFanoStore, numQueries, simultaneousQueries, keys);
        std::cout << std::endl;
    }
    {
        SeparatorHashing<6, VerboseBenchmarkConfig<IoManager>> separatorHashingStore(fillDegree, filename);
        testConstruct(separatorHashingStore, keys);
        testPerformQueries(separatorHashingStore, numQueries, simultaneousQueries, keys);
        std::cout << std::endl;
    }
    {
        ParallelCuckooHashing<VerboseBenchmarkConfig<IoManager>> cuckooHashing(fillDegree, filename);
        testConstruct(cuckooHashing, keys);
        testPerformQueries(cuckooHashing, numQueries, simultaneousQueries, keys);
        std::cout<<std::endl;
    }
}

int main() {
    testVariableSizeObjectStores<MemoryMapIO<>>(1e7, 0.98, 1);
    //testVariableSizeObjectStores<PosixIO<O_DIRECT | O_SYNC>>(1e7, 0.98, 1);
    //testVariableSizeObjectStores<PosixAIO<O_DIRECT | O_SYNC>>(1e7, 0.98, 1);
    //testVariableSizeObjectStores<UringIO<O_DIRECT | O_SYNC>>(1e7, 0.98, 1);
    return 0;
}
