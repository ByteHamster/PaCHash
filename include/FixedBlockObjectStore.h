#ifndef TESTCOMPARISON_FIXEDBLOCKOBJECTSTORE_H
#define TESTCOMPARISON_FIXEDBLOCKOBJECTSTORE_H

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
        size_t numBuckets;
        float fillDegree = 0;
        std::vector<FixedBlockObjectStore::Bucket> buckets;
    public:

        FixedBlockObjectStore(size_t numObjects, size_t averageSize, float fillDegree)
                : VariableSizeObjectStore(numObjects, averageSize), fillDegree(fillDegree) {
            size_t spaceNeeded = numObjects * averageSize;
            numBuckets = (spaceNeeded / fillDegree) / PageConfig::PAGE_SIZE;
        }

        void writeBuckets() {
            size_t objectsWritten = 0;
            auto myfile = std::fstream(INPUT_FILE, std::ios::out | std::ios::binary | std::ios::trunc);
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                assert(myfile.tellg() == bucket * PageConfig::PAGE_SIZE);
                size_t written = 0;
                for (Item &item : buckets.at(bucket).items) {
                    std::string value = Distribution::valueFor<distribution>(item.key, averageSize);
                    assert(value.length() == item.length);
                    ObjectHeader header = {item.key, static_cast<uint16_t>(value.length())};
                    myfile.write(reinterpret_cast<const char *>(&header), sizeof(ObjectHeader));
                    myfile.write(value.data(), value.length());
                    written += sizeof(ObjectHeader) + value.length();
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

                if (SHOW_PROGRESS && (bucket % (numBuckets/PROGRESS_STEPS) == 0 || bucket == numBuckets - 1)) {
                    std::cout<<"\r# Writing "<<std::round(100.0*bucket/numBuckets)<<"%"<<std::flush;
                }
            }
            assert(objectsWritten == numObjects);
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

#endif //TESTCOMPARISON_FIXEDBLOCKOBJECTSTORE_H
