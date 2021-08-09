#ifndef TESTCOMPARISON_VARIABLESIZEOBJECTSTORE_H
#define TESTCOMPARISON_VARIABLESIZEOBJECTSTORE_H

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>
#include <functional>

#include "QueryTimer.h"

struct ObjectHeader {
    uint64_t key;
    uint16_t length;
};

class VariableSizeObjectStore {
    public:
        inline static const char* INPUT_FILE = "key_value_store.txt";
        static constexpr bool SHOW_PROGRESS = true;
        static constexpr int PROGRESS_STEPS = 20;

        const size_t numObjects = 0;
        const size_t averageSize = 0;

        VariableSizeObjectStore(size_t numObjects, size_t averageSize)
                : numObjects(numObjects), averageSize(averageSize) {
        }

        /**
         * Write the objects to disk.
         * @param keysAndLengths The list of keys to store
         * @param valuePointer After the store is done calculating the file structure, it uses this callback function
         *                     to obtain pointers to the object values. Not having to pass the values explicitly
         *                     has the advantage that the values do not need to be all present during construction.
         */
        virtual void generateInputData(std::vector<std::pair<uint64_t, size_t>> &keysAndLengths,
                                       std::function<const char*(uint64_t)> valuePointer) = 0;
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
