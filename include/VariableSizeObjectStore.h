#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>
#include <functional>

#include "QueryTimer.h"
#include "IoManager.h"

struct ObjectHeader {
    uint64_t key;
    uint16_t length;
};

struct VariableSizeObjectStoreConfig {
    static constexpr bool SHOW_PROGRESS = false;
    static constexpr int PROGRESS_STEPS = 1;
    using IoManager = MemoryMapIO<0>;
};

template <typename Config_ = VariableSizeObjectStoreConfig>
class VariableSizeObjectStore {
    public:
        using Config = Config_;
        const char* filename;

        const size_t numObjects = 0;
        const size_t averageSize = 0;

        VariableSizeObjectStore(size_t numObjects, size_t averageSize, const char* filename)
                : numObjects(numObjects), averageSize(averageSize), filename(filename) {
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

        static inline void LOG(const char *step, size_t progress = -1, size_t max = -1) {
            if constexpr (Config::SHOW_PROGRESS) {
                if (step == nullptr) {
                    std::cout<<"\r"<<std::flush;
                } else if (progress == -1) {
                    std::cout<<"\r# "<<step<<std::flush;
                } else if ((progress % (max/Config::PROGRESS_STEPS)) == 0 || progress == max - 1) {
                    std::cout<<"\r# "<<step<<" ("<<std::round(100.0*(double)progress/(double)max)<<"%)"<<std::flush;
                }
            }
        }
};
