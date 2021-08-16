#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>

class FixedBlockObjectStore : public VariableSizeObjectStore {
    public:
        struct Item {
            uint64_t key = 0;
            size_t length = 0;
            size_t currentHashFunction = 0;
        };
        struct Bucket {
            std::vector<Item> items;
            size_t length = 0;
        };
    protected:
        size_t numBuckets = 0;
        const float fillDegree;
        std::vector<FixedBlockObjectStore::Bucket> buckets;
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
            spaceNeeded += keys.size() * sizeof(ObjectHeader);
            this->numBuckets = (spaceNeeded / this->fillDegree) / PageConfig::PAGE_SIZE;
            this->buckets.resize(this->numBuckets);
        }

    protected:
        void writeBuckets(ObjectProvider &objectProvider) {
            size_t objectsWritten = 0;
            auto myfile = std::fstream(this->filename, std::ios::out | std::ios::binary | std::ios::trunc);
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                assert(myfile.tellg() == bucket * PageConfig::PAGE_SIZE);
                size_t written = 0;
                for (Item &item : buckets.at(bucket).items) {
                    ObjectHeader header = {item.key, static_cast<uint16_t>(item.length)};
                    myfile.write(reinterpret_cast<const char *>(&header), sizeof(ObjectHeader));
                    myfile.write(objectProvider.getValue(item.key), item.length);
                    written += sizeof(ObjectHeader) + item.length;
                    assert(written <= PageConfig::PAGE_SIZE);
                    objectsWritten++;
                }
                size_t freeSpaceLeft = PageConfig::PAGE_SIZE - written;
                if (freeSpaceLeft >= sizeof(ObjectHeader)) {
                    ObjectHeader header = {0, 0};
                    myfile.write(reinterpret_cast<const char *>(&header), sizeof(ObjectHeader));
                    freeSpaceLeft -= sizeof(ObjectHeader);
                }
                if (freeSpaceLeft > 0) {
                    myfile.seekp(freeSpaceLeft, std::_S_cur);
                }
                this->LOG("Writing", bucket, numBuckets);
            }
            assert(objectsWritten == this->numObjects);
            myfile.flush();
            myfile.sync();
            myfile.close();
        }

        std::tuple<size_t, char *> findKeyWithinBlock(uint64_t key, char *block) {
            size_t position = 0;
            while (position + sizeof(ObjectHeader) < PageConfig::PAGE_SIZE) {
                ObjectHeader *header = reinterpret_cast<ObjectHeader *>(&block[position]);
                if (header->key == 0 && header->length == 0) {
                    break; // End of block
                }
                if (header->key == key) {
                    return std::make_tuple(header->length, &block[position + sizeof(ObjectHeader)]);
                }
                position += sizeof(ObjectHeader) + header->length;
            }
            return std::make_tuple(0, nullptr);
        }
};
