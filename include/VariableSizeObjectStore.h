#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>
#include <functional>

#include "QueryTimer.h"
#include "IoManager.h"

class ObjectProvider {
    public:
        /**
         * Returns the size of an object. The returned size must remain constant.
         */
        [[nodiscard]] virtual size_t getLength(uint64_t key) = 0;

        /**
         * Returns a pointer to the value of the object. This method is called lazily when writing the objects,
         * so it is not necessary for the value of all objects to be available at the same time.
         * The pointer is assumed to be valid until the next call to getValue().
         */
        [[nodiscard]] virtual const char *getValue(uint64_t key) = 0;
};

class VariableSizeObjectStore {
    public:
        struct QueryHandle;
    protected:
        static constexpr bool SHOW_PROGRESS = true;
        static constexpr int PROGRESS_STEPS = 4;
        static constexpr size_t overheadPerObject = sizeof(uint16_t) + sizeof(uint64_t); // Key and length
        static constexpr size_t overheadPerPage = 2*sizeof(uint16_t); // Number of objects and offset
        using MetadataObjectType = size_t;

        struct Item {
            uint64_t key = 0;
            size_t length = 0;
            size_t currentHashFunction = 0;
        };
        struct Bucket {
            std::vector<Item> items;
            size_t length = 0;
        };
        const char* filename;
        size_t numObjects = 0;
        std::vector<Bucket> buckets;
        size_t numBuckets = 0;
        const float fillDegree;
        size_t totalPayloadSize = 0;
    public:

        explicit VariableSizeObjectStore(float fillDegree, const char* filename)
            : filename(filename), fillDegree(fillDegree) {
        }

        virtual ~VariableSizeObjectStore() = default;

        /**
         * Write the objects to disk.
         * @param keys The list of keys to store.
         * @param objectProvider The provider allows to access the keys and their lengths.
         */
        virtual void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) = 0;

        /**
         * Reload the data structure from the file and construct the internal-memory data structures.
         */
        virtual void reloadFromFile() = 0;

        /**
         * Space usage per block, in bits.
         */
        virtual float internalSpaceUsage() = 0;

        virtual void printConstructionStats() {
            std::cout<<"External space usage: "<<prettyBytes(numBuckets*PageConfig::PAGE_SIZE)<<std::endl;
            std::cout<<"External utilization: "
                     <<std::round(100.0*totalPayloadSize/(numBuckets*PageConfig::PAGE_SIZE)*10)/10<<"%, "
                     <<"with keys: "<<std::round(100.0*(totalPayloadSize + numObjects*sizeof(uint64_t))/(numBuckets*PageConfig::PAGE_SIZE)*10)/10<<"%, "
                     <<"with keys+length: "<<std::round(100.0*(totalPayloadSize + numObjects*(sizeof(uint64_t)+sizeof(uint16_t)))/(numBuckets*PageConfig::PAGE_SIZE)*10)/10<<"%, "
                     <<"target: "<<std::round(100*fillDegree*10)/10<<"%"<<std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/numObjects<<std::endl;
        }

        virtual void printQueryStats() = 0;

        inline static void LOG(const char *step, size_t progress = -1, size_t max = -1) {
            if constexpr (SHOW_PROGRESS) {
                if (step == nullptr) {
                    std::cout<<"\r\033[K"<<std::flush;
                } else if (progress == -1) {
                    std::cout<<"\r\033[K# "<<step<<std::flush;
                } else if ((progress % (max/PROGRESS_STEPS + 1)) == 0 || progress == max - 1) {
                    std::cout<<"\r\033[K# "<<step<<" ("<<std::round(100.0*(double)progress/(double)max)<<"%)"<<std::flush;
                }
            }
        }
    protected:
        virtual void submitQuery(QueryHandle &handle) = 0;
        virtual void awaitCompletion(QueryHandle &handle) = 0;

        size_t blockHeaderSize(size_t block) {
            uint16_t numObjectsInBlock = block < buckets.size() ? buckets.at(block).items.size() : 0;
            if (block == 0) {
                numObjectsInBlock++;
            }
            return overheadPerPage + numObjectsInBlock*overheadPerObject;
        }

        void writeBuckets(ObjectProvider &objectProvider, bool allowOverlap) {
            size_t objectsWritten = 0;
            uint16_t offset = 0;
            auto myfile = std::fstream(this->filename, std::ios::out | std::ios::binary | std::ios::trunc);
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                assert(myfile.tellg() == bucket * PageConfig::PAGE_SIZE);

                uint16_t numObjectsInBlock = buckets.at(bucket).items.size();
                if (bucket == 0) {
                    numObjectsInBlock++;
                }
                myfile.write(reinterpret_cast<const char *>(&offset), sizeof(uint16_t));
                myfile.write(reinterpret_cast<const char *>(&numObjectsInBlock), sizeof(uint16_t));

                // Write lengths
                if (bucket == 0) {
                    uint16_t length = sizeof(MetadataObjectType); // Special object that contains metadata
                    myfile.write(reinterpret_cast<const char *>(&length), sizeof(uint16_t));
                }
                for (Item &item : buckets.at(bucket).items) {
                    uint16_t length = item.length;
                    myfile.write(reinterpret_cast<const char *>(&length), sizeof(uint16_t));
                }

                // Write keys
                if (bucket == 0) {
                    uint64_t key = 0; // Special object that contains metadata
                    myfile.write(reinterpret_cast<const char *>(&key), sizeof(uint64_t));
                }
                for (Item &item : buckets.at(bucket).items) {
                    uint64_t key = item.key;
                    myfile.write(reinterpret_cast<const char *>(&key), sizeof(uint64_t));
                }

                // Write object contents
                size_t written = blockHeaderSize(bucket);
                if (offset > 0) {
                    myfile.seekg(offset, std::ios::cur);
                    written += offset;
                }
                assert(myfile.tellg() == bucket*PageConfig::PAGE_SIZE + written);
                if (bucket == 0) {
                    MetadataObjectType value = numBuckets; // Special object that contains metadata
                    myfile.write(reinterpret_cast<const char *>(&value), sizeof(MetadataObjectType));
                    written += sizeof(MetadataObjectType);
                }
                size_t nextOffset = 0;
                for (size_t i = 0; i < buckets.at(bucket).items.size(); i++) {
                    Item &item = buckets.at(bucket).items.at(i);
                    if (allowOverlap) {
                        size_t freeSpaceLeft = PageConfig::PAGE_SIZE - written;
                        if (item.length <= freeSpaceLeft) {
                            myfile.write(objectProvider.getValue(item.key), item.length);
                            written += item.length;
                            assert(written <= (bucket+1)*PageConfig::PAGE_SIZE);
                        } else {
                            // Write into next block. Only the last object of each page can overlap.
                            assert(i == buckets.at(bucket).items.size() - 1);
                            const char *objectContent = objectProvider.getValue(item.key);
                            myfile.write(objectContent, freeSpaceLeft);
                            off_t nextBlockHeaderSize = blockHeaderSize(bucket + 1);
                            size_t itemRemaining = item.length - freeSpaceLeft;
                            assert(nextBlockHeaderSize + itemRemaining <= PageConfig::PAGE_SIZE);
                            myfile.seekg(nextBlockHeaderSize, std::ios::cur);
                            myfile.write(objectContent + freeSpaceLeft, itemRemaining);
                            myfile.seekg(-nextBlockHeaderSize - itemRemaining, std::ios::cur);
                            written += item.length;
                            nextOffset = itemRemaining;
                        }
                    } else {
                        myfile.write(objectProvider.getValue(item.key), item.length);
                        written += item.length;
                        assert(written <= PageConfig::PAGE_SIZE);
                    }
                    objectsWritten++;
                }
                size_t expectedWritten = buckets.at(bucket).length + overheadPerPage;
                if (bucket == 0) {
                    expectedWritten += sizeof(MetadataObjectType) + overheadPerObject;
                }
                assert(written - offset == expectedWritten);
                offset = nextOffset;

                if (written < PageConfig::PAGE_SIZE) {
                    size_t freeSpaceLeft = PageConfig::PAGE_SIZE - written;
                    myfile.seekp(freeSpaceLeft, std::ios::cur);
                }
                this->LOG("Writing", bucket, numBuckets);
            }
            assert(objectsWritten == this->numObjects);
            LOG("Flushing and closing file");
            myfile.flush();
            myfile.sync();
            myfile.close();
        }

        static std::tuple<size_t, char *> findKeyWithinBlock(uint64_t key, char *block) {
            uint16_t offset = *reinterpret_cast<uint16_t *>(&block[0]);
            uint16_t numObjects = *reinterpret_cast<uint16_t *>(&block[0 + sizeof(uint16_t)]);

            for (size_t i = 0; i < numObjects; i++) {
                uint64_t keyFound = *reinterpret_cast<uint64_t *>(&block[overheadPerPage + numObjects*sizeof(uint16_t) + i*sizeof(uint64_t)]);
                if (key == keyFound) {
                    uint16_t length = *reinterpret_cast<uint16_t *>(&block[overheadPerPage + i*sizeof(uint16_t)]);
                    for (size_t k = 0; k < i; k++) {
                        offset += *reinterpret_cast<uint16_t *>(&block[overheadPerPage + k*sizeof(uint16_t)]);
                    }
                    return std::make_tuple(length, &block[overheadPerPage + numObjects*overheadPerObject + offset]);
                }
            }
            return std::make_tuple(0, nullptr);
        }

        static MetadataObjectType readSpecialObject0(const char *filename) {
            int fd = open(filename, O_RDONLY);
            char *fileFirstPage = static_cast<char *>(mmap(nullptr, PageConfig::PAGE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0));
            uint16_t numObjectsOnPage = *reinterpret_cast<uint16_t *>(&fileFirstPage[sizeof(uint16_t)]);
            uint16_t objectStart = numObjectsOnPage*overheadPerObject;
            MetadataObjectType numBucketsRead = *reinterpret_cast<MetadataObjectType *>(&fileFirstPage[overheadPerPage + objectStart]);
            munmap(fileFirstPage, PageConfig::PAGE_SIZE);
            close(fd);
            return numBucketsRead;
        }
    public:
        struct QueryHandle {
            VariableSizeObjectStore &owner;
            bool completed = true;
            std::vector<uint64_t> keys;
            std::vector<size_t> resultLengths;
            std::vector<char *> resultPointers;
            QueryTimer stats;
            std::unique_ptr<IoManager> ioManager;

            explicit QueryHandle(VariableSizeObjectStore &owner, size_t batchSize) : owner(owner) {
                keys.resize(batchSize);
                resultLengths.resize(batchSize);
                resultPointers.resize(batchSize);
            }

            void submit() {
                owner.submitQuery(*this);
            }

            void awaitCompletion() {
                owner.awaitCompletion(*this);
            }
        };
};
