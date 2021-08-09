#ifndef TESTCOMPARISON_VARIABLESIZEOBJECTSTORE_H
#define TESTCOMPARISON_VARIABLESIZEOBJECTSTORE_H

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>

#include "QueryTimer.h"

struct ObjectHeader {
    uint64_t key;
    uint16_t length;
};

class Distribution {
    public:
        static constexpr int EQUAL = 1;
        static constexpr int NORMAL = 2;
        static constexpr int EXPONENTIAL = 3;

        template <int distribution>
        static uint64_t sample(size_t averageLength, std::default_random_engine &prng) {
            averageLength -= sizeof(ObjectHeader);
            if constexpr (distribution == Distribution::EQUAL) {
                return averageLength;
            } else if constexpr (distribution == Distribution::NORMAL) {
                std::normal_distribution<double> normalDist(averageLength, 1.0);
                return normalDist(prng);
            } else if constexpr (distribution == Distribution::EXPONENTIAL) {
                double stretch = averageLength/2;
                double lambda = 1.0;
                std::exponential_distribution<double> expDist(lambda);
                return (averageLength - stretch/lambda) + stretch*expDist(prng);
            } else {
                assert(false && "Invalid distribution");
                return 0;
            }
        }

        template <int distribution>
        static size_t lengthFor(uint64_t key, size_t averageLength) {
            std::default_random_engine generator(key);
            return Distribution::sample<distribution>(averageLength, generator);
        }

        template <int distribution>
        static std::string valueFor(uint64_t key, size_t averageLength) {
            size_t length = Distribution::lengthFor<distribution>(key, averageLength);
            assert(length > 9);
            char string[length];
            string[0] = '_';
            *reinterpret_cast<uint64_t *>(&string[1]) = key;
            memset(&string[9], '.', length-9);
            return std::string(string, length);
        }
};

class VariableSizeObjectStore {
    public:
        inline static const char* INPUT_FILE = "/data01/hplehmann/key_value_store.txt";
        static constexpr bool SHOW_PROGRESS = true;
        static constexpr int PROGRESS_STEPS = 20;
        static constexpr int distribution = Distribution::NORMAL;

        const size_t numObjects = 0;
        const size_t averageSize = 0;
        std::vector<uint64_t> keysTestingOnly;

        VariableSizeObjectStore(size_t numObjects, size_t averageSize)
                : numObjects(numObjects), averageSize(averageSize) {
            keysTestingOnly.reserve(numObjects);
        }

        virtual void generateInputData() = 0;
        virtual void reloadInputDataFromFile() = 0;
        virtual void printConstructionStats() = 0;

        /**
         * Returns the size and a pointer to the value of all objects that were requested.
         * The pointers are valid until query() is called the next time. Can at most answer
         * PageConfig::MAX_SIMULTANEOUS_QUERIES queries at once.
         */
        virtual std::vector<std::tuple<size_t, char *>> query(std::vector<uint64_t> &keys) = 0;
        virtual void printQueryStats() = 0;
};

#endif //TESTCOMPARISON_VARIABLESIZEOBJECTSTORE_H
