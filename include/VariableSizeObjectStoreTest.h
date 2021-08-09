#ifndef TESTCOMPARISON_VARIABLESIZEOBJECTSTORETEST_H
#define TESTCOMPARISON_VARIABLESIZEOBJECTSTORETEST_H

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>

#include "EliasFanoIndexing.h"
#include "SeparatorHashing.h"
#include "ParallelCuckooHashing.h"

template <class T>
void testConstruct(T &objectStore) {
    static_assert(std::is_base_of<VariableSizeObjectStore, T>::value);
    auto time1 = std::chrono::high_resolution_clock::now();
    objectStore.generateInputData();
    auto time2 = std::chrono::high_resolution_clock::now();
    objectStore.reloadInputDataFromFile();
    auto time3 = std::chrono::high_resolution_clock::now();
    objectStore.printConstructionStats();

    std::cout<<"Construction duration: "
         <<std::chrono::duration_cast<std::chrono::milliseconds>(time2 - time1).count() << " ms writing, "
         <<std::chrono::duration_cast<std::chrono::milliseconds>(time3 - time2).count() << " ms reloading"<<std::endl;
}

template <class T>
void testPerformQueries(T &objectStore, size_t numQueries, size_t simultaneousQueries) {
    static_assert(std::is_base_of<VariableSizeObjectStore, T>::value);
    std::cout << "\r# Syncing filesystem before query"<<std::flush;
    system("sync");
    for (int q = 0; q < numQueries; q++) {
        std::vector<uint64_t> keys;
        for (int i = 0; i < simultaneousQueries; i++) {
            keys.push_back(objectStore.keysTestingOnly.at(rand() % objectStore.numObjects));
        }
        auto result = objectStore.query(keys);
        for (int i = 0; i < keys.size(); i++) {
            const auto& [length, valuePtr] = result.at(i);
            if (valuePtr == nullptr) {
                std::cerr<<"Object not found"<<std::endl;
            }
            if (std::string(valuePtr, length) != Distribution::valueFor<T::distribution>(keys.at(i), objectStore.averageSize)) {
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
    size_t numQueries = 5e4 / simultaneousQueries;
    {
        EliasFanoIndexing<8, IoManager> eliasFanoStore(numObjects, averageLength);
        testConstruct(eliasFanoStore);
        testPerformQueries(eliasFanoStore, numQueries, simultaneousQueries);
        std::cout << std::endl;
    }
    {
        SeparatorHashing<6, IoManager> separatorHashingStore(numObjects, averageLength, fillDegree);
        testConstruct(separatorHashingStore);
        testPerformQueries(separatorHashingStore, numQueries, simultaneousQueries);
        std::cout << std::endl;
    }
    {
        ParallelCuckooHashing<IoManager> cuckooHashing(numObjects, averageLength, fillDegree);
        testConstruct(cuckooHashing);
        testPerformQueries(cuckooHashing, numQueries, simultaneousQueries);
        std::cout<<std::endl;
    }
}

#endif //TESTCOMPARISON_VARIABLESIZEOBJECTSTORETEST_H
