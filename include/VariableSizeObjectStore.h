#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>
#include <functional>

#include "QueryTimer.h"
#include "ConstructionTimer.h"
#include "IoManager.h"
#include "Util.h"

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
        ConstructionTimer constructionTimer;
        struct QueryHandle {
            bool successful = false;
            uint64_t key = 0;
            size_t length = 0;
            char *resultPtr = nullptr;
            char *buffer = nullptr;
            QueryTimer stats;
            uint16_t state = 0;
            // Can be used freely by users to identify handles in the awaitAny method.
            uint64_t name = 0;
        };
        IoManager *ioManager = nullptr;
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

        virtual ~VariableSizeObjectStore() {
            delete ioManager;
        };

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

        virtual size_t requiredBufferPerQuery() = 0;

        virtual void submitSingleQuery(QueryHandle *handle) = 0;
        virtual QueryHandle *awaitAny() = 0;

        void submitQuery(QueryHandle *handle) {
            submitSingleQuery(handle);
            ioManager->submit();
        }

        void submitQueries(std::vector<QueryHandle*> &handles) {
            for (auto & handle : handles) {
                submitSingleQuery(handle);
            }
            ioManager->submit();
        }

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

            int fd = open(filename, O_RDWR | O_CREAT, 0600);
            if (fd < 0) {
                std::cerr<<"Error opening output file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            uint64_t fileSize = (numBuckets + 1)*PageConfig::PAGE_SIZE;
            if (ftruncate(fd, fileSize) < 0) {
                std::cerr<<"ftruncate: "<<strerror(errno)<<". If this is a partition, it can be ignored."<<std::endl;
            }
            char *file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
            if (file == MAP_FAILED) {
                std::cerr<<"Map output file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            madvise(file, fileSize, MADV_SEQUENTIAL);

            for (int bucket = 0; bucket < numBuckets; bucket++) {
                char *page = file + bucket*PageConfig::PAGE_SIZE;
                uint16_t numObjectsInBlock = buckets.at(bucket).items.size();
                if (bucket == 0) {
                    numObjectsInBlock++;
                }
                *reinterpret_cast<uint16_t *>(&page[0]) = offset;
                *reinterpret_cast<uint16_t *>(&page[sizeof(uint16_t)]) = numObjectsInBlock;

                uint16_t *lengths = reinterpret_cast<uint16_t *>(&page[2*sizeof(uint16_t)]);
                // Write lengths
                if (bucket == 0) {
                    *(lengths++) = sizeof(MetadataObjectType); // Special object that contains metadata
                }
                for (Item &item : buckets.at(bucket).items) {
                    *(lengths++) = item.length;
                }

                uint64_t *keys = reinterpret_cast<uint64_t *>(lengths);
                // Write keys
                if (bucket == 0) {
                    *(keys++) = 0; // Special object that contains metadata
                }
                for (Item &item : buckets.at(bucket).items) {
                    *(keys++) = item.key;
                }
                assert(blockHeaderSize(bucket) == ((long)keys - (long)page));

                // Write object contents
                char *content = reinterpret_cast<char *>(keys) + offset;
                if (bucket == 0) {
                    MetadataObjectType value = numBuckets; // Special object that contains metadata
                    memcpy(content, &value, sizeof(MetadataObjectType));
                    content += sizeof(MetadataObjectType);
                }
                size_t nextOffset = 0;
                for (size_t i = 0; i < buckets.at(bucket).items.size(); i++) {
                    Item &item = buckets.at(bucket).items.at(i);
                    size_t freeSpaceLeft = (size_t)page + PageConfig::PAGE_SIZE - (size_t)content;
                    if (item.length <= freeSpaceLeft) {
                        memcpy(content, objectProvider.getValue(item.key), item.length);
                        content += item.length;
                        assert((long)content-(long)page <= PageConfig::PAGE_SIZE);
                    } else if (allowOverlap) {
                        // Write into next block. Only the last object of each page can overlap.
                        assert(i == buckets.at(bucket).items.size() - 1);
                        const char *objectContent = objectProvider.getValue(item.key);
                        memcpy(content, objectContent, freeSpaceLeft);
                        off_t nextBlockHeaderSize = blockHeaderSize(bucket + 1);
                        size_t itemRemaining = item.length - freeSpaceLeft;
                        assert(nextBlockHeaderSize + itemRemaining <= PageConfig::PAGE_SIZE);
                        char *nextPage = page + PageConfig::PAGE_SIZE;
                        memcpy(nextPage+nextBlockHeaderSize, objectContent + freeSpaceLeft, itemRemaining);
                        nextOffset = itemRemaining;
                    } else {
                        std::cerr<<"Overlap but not allowed"<<std::endl;
                        exit(1);
                    }
                    objectsWritten++;
                }
                offset = nextOffset;
                LOG("Writing", bucket, numBuckets);
            }
            assert(objectsWritten == this->numObjects);
            LOG("Flushing and closing file");
            munmap(file, fileSize);
            close(fd);
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
};
