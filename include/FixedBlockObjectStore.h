#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>

class FixedBlockObjectStore : public VariableSizeObjectStore {
    protected:
        const float fillDegree;
    public:

        FixedBlockObjectStore(float fillDegree, const char* filename)
                : VariableSizeObjectStore(filename), fillDegree(fillDegree) {
        }

        void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) override {
            this->LOG("Calculating total size to determine number of blocks");
            this->numObjects = keys.size();
            size_t spaceNeeded = 0;
            for (unsigned long key : keys) {
                spaceNeeded += objectProvider.getLength(key);
            }
            spaceNeeded += keys.size() * (sizeof(uint64_t) + sizeof(uint16_t));
            this->numBuckets = (spaceNeeded / this->fillDegree) / PageConfig::PAGE_SIZE;
            this->buckets.resize(this->numBuckets);
        }
};
