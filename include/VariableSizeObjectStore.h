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
        [[nodiscard]] virtual StoreConfig::length_t getLength(StoreConfig::key_t key) = 0;

        /**
         * Returns a pointer to the value of the object. This method is called lazily when writing the objects,
         * so it is not necessary for the value of all objects to be available at the same time.
         * The pointer is assumed to be valid until the next call to getValue().
         */
        [[nodiscard]] virtual const char *getValue(StoreConfig::key_t key) = 0;
};

class VariableSizeObjectStore {
    public:
        ConstructionTimer constructionTimer;
        struct QueryHandle {
            bool successful = false;
            StoreConfig::key_t key = 0;
            StoreConfig::length_t length = 0;
            char *resultPtr = nullptr;
            char *buffer = nullptr;
            QueryTimer stats;
            uint16_t state = 0;
            // Can be used freely by users to identify handles in the awaitAny method.
            uint64_t name = 0;
        };
        const char* filename;
        static constexpr size_t overheadPerObject = sizeof(StoreConfig::key_t) + sizeof(StoreConfig::length_t);
        static constexpr size_t overheadPerBlock = 2 * sizeof(StoreConfig::length_t); // Number of objects and offset
        static constexpr bool SHOW_PROGRESS = true;
        static constexpr int PROGRESS_STEPS = 32;
        using MetadataObjectType = size_t;

        struct Item {
            StoreConfig::key_t key = 0;
            StoreConfig::length_t length = 0;
            uint64_t userData = 0; // Eg. number of hash function
        };
        struct Block {
            std::vector<Item> items;
            size_t length = 0;
        };
        class BlockStorage;
    protected:
        size_t numObjects = 0;
        std::vector<Block> blocks;
        size_t numBlocks = 0;
        const float fillDegree;
        size_t totalPayloadSize = 0;
        int openFlags;
    public:

        explicit VariableSizeObjectStore(float fillDegree, const char* filename, int openFlags)
            : filename(filename), fillDegree(fillDegree), openFlags(openFlags) {
        }

        /**
         * Write the objects to disk.
         * @param keys The list of keys to store.
         * @param objectProvider The provider allows to access the keys and their lengths.
         */
        virtual void writeToFile(std::vector<StoreConfig::key_t> &keys, ObjectProvider &objectProvider) = 0;

        /**
         * Reload the data structure from the file and construct the internal-memory data structures.
         */
        virtual void reloadFromFile() = 0;

        /**
         * Space usage per block, in bits.
         */
        virtual float internalSpaceUsage() = 0;

        virtual void printConstructionStats() {
            std::cout << "External space usage: " << prettyBytes(numBlocks * StoreConfig::BLOCK_LENGTH) << std::endl;
            std::cout << "External utilization: "
                      << std::round(100.0 * totalPayloadSize / (numBlocks * StoreConfig::BLOCK_LENGTH) * 10) / 10 << "%, "
                      << "with keys: " << std::round(100.0 * (totalPayloadSize + numObjects*sizeof(uint64_t)) / (numBlocks * StoreConfig::BLOCK_LENGTH) * 10) / 10 << "%, "
                      << "with keys+length: " << std::round(100.0 * (totalPayloadSize + numObjects*(sizeof(uint64_t)+sizeof(uint16_t))) / (numBlocks * StoreConfig::BLOCK_LENGTH) * 10) / 10 << "%, "
                      << "target: " <<std::round(100*fillDegree*10)/10 << "%" << std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/numObjects<<std::endl;
        }

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

        static std::tuple<StoreConfig::length_t, char *> findKeyWithinBlock(StoreConfig::key_t key, char *data) {
            BlockStorage block(data);
            char *objectPointer = block.objectsStart;
            for (size_t i = 0; i < block.numObjects; i++) {
                if (key == block.keys[i]) {
                    return std::make_tuple(block.lengths[i], objectPointer);
                }
                objectPointer += block.lengths[i];
            }
            return std::make_tuple(0, nullptr);
        }

        static MetadataObjectType readSpecialObject0(const char *filename) {
            int fd = open(filename, O_RDONLY);
            if (fd < 0) {
                std::cerr<<"File not found"<<std::endl;
                exit(1);
            }
            char *fileFirstPage = static_cast<char *>(mmap(nullptr, StoreConfig::BLOCK_LENGTH, PROT_READ, MAP_PRIVATE, fd, 0));
            BlockStorage block(fileFirstPage);
            MetadataObjectType numBlocksRead = *reinterpret_cast<MetadataObjectType *>(&block.objectsStart[0]);
            assert(block.keys[0] == 0);
            munmap(fileFirstPage, StoreConfig::BLOCK_LENGTH);
            close(fd);
            return numBlocksRead;
        }

        class BlockStorage {
            public:
                char *blockStart;
                StoreConfig::length_t offset;
                StoreConfig::length_t numObjects;
                char *tableStart;
                StoreConfig::length_t *lengths;
                StoreConfig::key_t *keys;
                char *objectsStart;

                explicit BlockStorage(char *data)
                        : blockStart(data),
                          offset(*reinterpret_cast<StoreConfig::length_t *>(&data[StoreConfig::BLOCK_LENGTH - sizeof(StoreConfig::length_t)])),
                          numObjects(*reinterpret_cast<StoreConfig::length_t *>(&data[StoreConfig::BLOCK_LENGTH - 2 * sizeof(StoreConfig::length_t)])),
                          tableStart(&data[StoreConfig::BLOCK_LENGTH - overheadPerBlock - numObjects * overheadPerObject]),
                          lengths(reinterpret_cast<StoreConfig::length_t *>(&tableStart[numObjects * sizeof(StoreConfig::key_t)])),
                          keys(reinterpret_cast<StoreConfig::key_t *>(tableStart)),
                          objectsStart(&data[offset]) {
                    assert(numObjects < StoreConfig::BLOCK_LENGTH);
                }

                explicit BlockStorage()
                    : blockStart(nullptr), offset(0), numObjects(0),
                    tableStart(nullptr), lengths(nullptr), keys(nullptr), objectsStart(nullptr) {
                }

                static BlockStorage init(char *data, StoreConfig::length_t offset, StoreConfig::length_t numObjects) {
                    assert(offset < StoreConfig::BLOCK_LENGTH);
                    assert(numObjects < StoreConfig::BLOCK_LENGTH);
                    *reinterpret_cast<StoreConfig::length_t *>(&data[StoreConfig::BLOCK_LENGTH - sizeof(StoreConfig::length_t)]) = offset;
                    *reinterpret_cast<StoreConfig::length_t *>(&data[StoreConfig::BLOCK_LENGTH - 2 * sizeof(StoreConfig::length_t)]) = numObjects;
                    return BlockStorage(data);
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
