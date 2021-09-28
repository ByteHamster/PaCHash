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
        const char* filename;
        static constexpr size_t overheadPerObject = sizeof(uint16_t) + sizeof(uint64_t); // Key and length
        static constexpr size_t overheadPerPage = 2*sizeof(uint16_t); // Number of objects and offset
        static constexpr bool SHOW_PROGRESS = true;
        static constexpr int PROGRESS_STEPS = 4;
        using MetadataObjectType = size_t;

        struct Item {
            uint64_t key = 0;
            size_t length = 0;
            uint64_t userData = 0; // Eg. number of hash function
        };
        struct Bucket {
            std::vector<Item> items;
            size_t length = 0;
        };
        class BlockStorage;
    protected:
        size_t numObjects = 0;
        std::vector<Bucket> buckets;
        size_t numBuckets = 0;
        const float fillDegree;
        size_t totalPayloadSize = 0;
    public:

        explicit VariableSizeObjectStore(float fillDegree, const char* filename)
            : filename(filename), fillDegree(fillDegree) {
        }

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
        virtual size_t requiredIosPerQuery() = 0;

        size_t writeBucket(Bucket &bucket, BlockStorage &storage, ObjectProvider &objectProvider,
                                  bool allowOverlap, size_t nextBucketSize) const {
            uint16_t numObjectsInBlock = bucket.items.size();

            for (size_t i = 0; i < numObjectsInBlock; i++) {
                storage.lengths[i] = bucket.items.at(i).length;
            }
            for (size_t i = 0; i < numObjectsInBlock; i++) {
                storage.keys[i] = bucket.items.at(i).key;
            }

            storage.calculateObjectPositions();
            for (size_t i = 0; i < numObjectsInBlock; i++) {
                Item &item = bucket.items.at(i);
                if (item.key == 0) { // Special object that contains metadata
                    MetadataObjectType value = numBuckets;
                    memcpy(storage.objects[i], &value, sizeof(MetadataObjectType));
                    continue;
                }

                const char *objectContent = objectProvider.getValue(item.key);
                size_t freeSpaceLeft = PageConfig::PAGE_SIZE - (storage.objects[i] - storage.data);
                memcpy(storage.objects[i], objectContent, std::min(item.length, freeSpaceLeft));

                if (freeSpaceLeft >= item.length) {
                    continue; // Done
                } else if (!allowOverlap) {
                    std::cerr<<"Error: Item does not fit onto page and overlap not allowed"<<std::endl;
                    exit(1);
                }
                // Write into next block. Only the last object of each page can overlap.
                assert(i == bucket.items.size() - 1);
                BlockStorage nextBlock = BlockStorage::init(storage.data + PageConfig::PAGE_SIZE,
                    item.length - freeSpaceLeft, nextBucketSize);
                memcpy(nextBlock.rawContent, objectContent + freeSpaceLeft, nextBlock.offset);
                return nextBlock.offset;
            }
            return 0;
        }

        void writeBuckets(ObjectProvider &objectProvider, bool allowOverlap) {
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

            Item firstMetadataItem = {0, sizeof(MetadataObjectType), 0};
            buckets.at(0).items.insert(buckets.at(0).items.begin(), firstMetadataItem);

            for (int bucketIdx = 0; bucketIdx < numBuckets; bucketIdx++) {
                Bucket &bucket = buckets.at(bucketIdx);
                BlockStorage storage = BlockStorage::init(file + bucketIdx*PageConfig::PAGE_SIZE, offset, bucket.items.size());
                offset = writeBucket(bucket, storage, objectProvider, allowOverlap,
                     (bucketIdx >= numBuckets - 1) ? 0 : buckets.at(bucketIdx + 1).items.size());
                LOG("Writing", bucketIdx, numBuckets);
            }
            LOG("Flushing and closing file");
            munmap(file, fileSize);
            close(fd);
        }

        static std::tuple<size_t, char *> findKeyWithinBlock(uint64_t key, char *data) {
            BlockStorage block(data);
            for (size_t i = 0; i < block.numObjects; i++) {
                if (key == block.keys[i]) {
                    block.calculateObjectPositions();
                    return std::make_tuple(block.lengths[i], block.objects[i]);
                }
            }
            return std::make_tuple(0, nullptr);
        }

        static MetadataObjectType readSpecialObject0(const char *filename) {
            int fd = open(filename, O_RDONLY);
            char *fileFirstPage = static_cast<char *>(mmap(nullptr, PageConfig::PAGE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0));
            BlockStorage block(fileFirstPage);
            MetadataObjectType numBucketsRead = *reinterpret_cast<MetadataObjectType *>(&block.content[0]);
            assert(block.keys[0] == 0);
            munmap(fileFirstPage, PageConfig::PAGE_SIZE);
            close(fd);
            return numBucketsRead;
        }

        class BlockStorage {
            public:
                char *data;
                const uint16_t offset;
                const uint16_t numObjects;
                uint16_t *lengths;
                uint64_t *keys;
                char *rawContent;
                char *content;
                char **objects = nullptr;

                explicit BlockStorage(char *data)
                        : data(data),
                          offset(*reinterpret_cast<uint16_t *>(&data[0])),
                          numObjects(*reinterpret_cast<uint16_t *>(&data[0 + sizeof(uint16_t)])),
                          lengths(reinterpret_cast<uint16_t *>(&data[overheadPerPage])),
                          keys(reinterpret_cast<uint64_t *>(&data[overheadPerPage + numObjects*sizeof(uint16_t)])),
                          rawContent(data + overheadPerPage + numObjects*overheadPerObject),
                          content(rawContent + offset) {
                    assert(numObjects < PageConfig::PAGE_SIZE);
                }

                static BlockStorage init(char *data, uint16_t offset, uint16_t numObjects) {
                    *reinterpret_cast<uint16_t *>(&data[0]) = offset;
                    *reinterpret_cast<uint16_t *>(&data[0 + sizeof(uint16_t)]) = numObjects;
                    return BlockStorage(data);
                }

                ~BlockStorage() {
                    delete[] objects;
                }

                void calculateObjectPositions() {
                    objects = new char*[numObjects];
                    objects[0] = content;
                    for (size_t i = 1; i < numObjects; i++) {
                        objects[i] = objects[i - 1] + lengths[i - 1];
                    }
                }
        };
};

/**
 * Can be used to query an object store.
 * Multiple Views can be opened on one single object store to support multi-threaded queries without locking.
 */
template <class ObjectStore, class IoManager>
class ObjectStoreView {
    public:
        ObjectStore *objectStore;
        IoManager ioManager;

        ObjectStoreView(ObjectStore &objectStore, int openFlags, size_t maxSimultaneousRequests)
            : objectStore(&objectStore),
              ioManager(objectStore.filename, openFlags, maxSimultaneousRequests * objectStore.requiredIosPerQuery()) {
        }

        inline void submitSingleQuery(VariableSizeObjectStore::QueryHandle *handle) {
            objectStore->submitSingleQuery(handle, &ioManager);
        }

        inline VariableSizeObjectStore::QueryHandle *awaitAny() {
            return objectStore->awaitAny(&ioManager);
        }

        inline VariableSizeObjectStore::QueryHandle *peekAny() {
            return objectStore->peekAny(&ioManager);
        }

        inline void submitQuery(VariableSizeObjectStore::QueryHandle *handle) {
            objectStore->submitSingleQuery(handle, &ioManager);
            ioManager.submit();
        }

        inline void submit() {
            ioManager.submit();
        }
};
