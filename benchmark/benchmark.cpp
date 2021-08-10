#include <IoManager.h>
#include <string>
#include <iostream>
#include <chrono>
#include <EliasFanoIndexing.h>
#include <SeparatorHashing.h>
#include <ParallelCuckooHashing.h>

#include "distribution.h"

const int TestDistribution = Distribution::NORMAL;
std::string temp;

template <typename IoManager_>
struct VerboseBenchmarkConfig {
    static constexpr bool SHOW_PROGRESS = true;
    static constexpr int PROGRESS_STEPS = 20;
    using IoManager = IoManager_;
};

template <class T>
void testConstruct(T &objectStore, std::vector<std::pair<uint64_t, size_t>> &keysAndLengths, size_t averageLength) {
    auto time1 = std::chrono::high_resolution_clock::now();
    objectStore.generateInputData(keysAndLengths, [&] (uint64_t key) {
        temp = Distribution::valueFor<TestDistribution>(key, averageLength);
        return temp.c_str();
    });
    auto time2 = std::chrono::high_resolution_clock::now();
    objectStore.reloadInputDataFromFile();
    auto time3 = std::chrono::high_resolution_clock::now();
    objectStore.printConstructionStats();

    std::cout<<"Construction duration: "
        <<std::chrono::duration_cast<std::chrono::milliseconds>(time2 - time1).count() << " ms writing, "
        <<std::chrono::duration_cast<std::chrono::milliseconds>(time3 - time2).count() << " ms reloading"<<std::endl;
}

template <class T>
void testPerformQueries(T &objectStore, size_t numQueries, size_t simultaneousQueries,
                        std::vector<std::pair<uint64_t, size_t>> &keysAndLengths) {
    T::LOG("Syncing filesystem before query");
    system("sync");
    for (int q = 0; q < numQueries; q++) {
        std::vector<uint64_t> keys;
        for (int i = 0; i < simultaneousQueries; i++) {
            keys.push_back(keysAndLengths.at(rand() % objectStore.numObjects).first);
        }
        auto result = objectStore.query(keys);
        for (int i = 0; i < keys.size(); i++) {
            const auto& [length, valuePtr] = result.at(i);
            if (valuePtr == nullptr) {
                std::cerr<<"Object not found"<<std::endl;
            }
            if (std::string(valuePtr, length) != Distribution::valueFor<TestDistribution>(keys.at(i), objectStore.averageSize)) {
                std::cerr<<"Unexpected result"<<std::endl;
            }
            T::LOG("Querying", q, numQueries);
        }
    }
    std::cout << "\r";
    objectStore.printQueryStats();
}

template<class IoManager = MemoryMapIO<>>
static void testVariableSizeObjectStores(size_t numObjects, size_t averageLength, float fillDegree, size_t simultaneousQueries) {
    std::cout<<"# Generating input keys"<<std::flush;
    std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    std::vector<std::pair<uint64_t, size_t>> keysAndLengths;
    keysAndLengths.reserve(numObjects);
    for (size_t i = 0; i < numObjects; i++) {
        uint64_t key = dist(generator);
        keysAndLengths.emplace_back(key, Distribution::lengthFor<TestDistribution>(key, averageLength));
    }
    std::cout << "\r"<<std::flush;

    size_t numQueries = 1e7 / simultaneousQueries;
    {
        EliasFanoIndexing<8, VerboseBenchmarkConfig<IoManager>> eliasFanoStore(numObjects, averageLength);
        testConstruct(eliasFanoStore, keysAndLengths, averageLength);
        testPerformQueries(eliasFanoStore, numQueries, simultaneousQueries, keysAndLengths);
        std::cout << std::endl;
    }
    /*{
        SeparatorHashing<6, VerboseBenchmarkConfig<IoManager>> separatorHashingStore(numObjects, averageLength, fillDegree);
        testConstruct(separatorHashingStore, keysAndLengths, averageLength);
        testPerformQueries(separatorHashingStore, numQueries, simultaneousQueries, keysAndLengths);
        std::cout << std::endl;
    }
    {
        ParallelCuckooHashing<VerboseBenchmarkConfig<IoManager>> cuckooHashing(numObjects, averageLength, fillDegree);
        testConstruct(cuckooHashing, keysAndLengths, averageLength);
        testPerformQueries(cuckooHashing, numQueries, simultaneousQueries, keysAndLengths);
        std::cout<<std::endl;
    }*/
}

int main() {
    testVariableSizeObjectStores<MemoryMapIO<>>(1e7, 256, 0.98, 1);
    //testVariableSizeObjectStores<PosixIO<O_DIRECT | O_SYNC>>(1e7, 256, 0.98, 1);
    //testVariableSizeObjectStores<PosixAIO<O_DIRECT | O_SYNC>>(1e7, 256, 0.98, 1);
    //testVariableSizeObjectStores<UringIO<>>(1e7, 256, 0.98, 1);
    return 0;
}
