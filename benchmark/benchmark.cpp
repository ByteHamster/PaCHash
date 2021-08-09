#include <IoManager.h>
#include <string>
#include <iostream>
#include <chrono>
#include <EliasFanoIndexing.h>
#include <SeparatorHashing.h>
#include <ParallelCuckooHashing.h>

#include "distribution.cpp"

const int TestDistribution = Distribution::NORMAL;
std::string temp;

template <class T>
void testConstruct(T &objectStore, std::vector<std::pair<uint64_t, size_t>> &keysAndLengths, size_t averageLength) {
    static_assert(std::is_base_of<VariableSizeObjectStore, T>::value);
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
    static_assert(std::is_base_of<VariableSizeObjectStore, T>::value);
    std::cout << "\r# Syncing filesystem before query"<<std::flush;
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
            if (((q % (numQueries/T::PROGRESS_STEPS)) == 1 || q == numQueries - 1) && T::SHOW_PROGRESS) {
                // When it is printed, the first query already went through
                std::cout<<"\r# Querying: "<<std::round(100.0*q/numQueries)<<"%"<<std::flush;
            }
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

    size_t numQueries = 1e3 / simultaneousQueries;
    {
        EliasFanoIndexing<8, IoManager> eliasFanoStore(numObjects, averageLength);
        testConstruct(eliasFanoStore, keysAndLengths, averageLength);
        testPerformQueries(eliasFanoStore, numQueries, simultaneousQueries, keysAndLengths);
        std::cout << std::endl;
    }
    {
        SeparatorHashing<6, IoManager> separatorHashingStore(numObjects, averageLength, fillDegree);
        testConstruct(separatorHashingStore, keysAndLengths, averageLength);
        testPerformQueries(separatorHashingStore, numQueries, simultaneousQueries, keysAndLengths);
        std::cout << std::endl;
    }
    {
        ParallelCuckooHashing<IoManager> cuckooHashing(numObjects, averageLength, fillDegree);
        testConstruct(cuckooHashing, keysAndLengths, averageLength);
        testPerformQueries(cuckooHashing, numQueries, simultaneousQueries, keysAndLengths);
        std::cout<<std::endl;
    }
}

int main() {
    testVariableSizeObjectStores<PosixIO<O_DIRECT | O_SYNC>>(1e7, 256, 0.98, 1);
    testVariableSizeObjectStores<PosixAIO<O_DIRECT | O_SYNC>>(1e7, 256, 0.98, 1);
    testVariableSizeObjectStores<UringIO<>>(1e7, 256, 0.98, 1);
    return 0;
}
