#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "StoreConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"

/**
 * Simple cuckoo hash table that loads both possible locations in parallel.
 */
class ParallelCuckooObjectStore : public VariableSizeObjectStore {
    private:
        using Super = VariableSizeObjectStore;
        using Item = typename Super::Item;
        std::vector<Item> insertionQueue;
    public:
        using QueryHandle = typename Super::QueryHandle;

        explicit ParallelCuckooObjectStore(float fillDegree, const char* filename, int openFlags)
                : VariableSizeObjectStore(fillDegree, filename, openFlags) {
        }

        static std::string name() {
            return "ParallelCuckooObjectStore";
        }

        void writeToFile(std::vector<StoreConfig::key_t> &keys, ObjectProvider &objectProvider) final {
            constructionTimer.notifyStartConstruction();
            LOG("Calculating total size to determine number of blocks");
            numObjects = keys.size();
            size_t spaceNeeded = 0;
            for (StoreConfig::key_t key : keys) {
                spaceNeeded += objectProvider.getLength(key);
            }
            totalPayloadSize = spaceNeeded;
            spaceNeeded += keys.size() * overheadPerObject;
            spaceNeeded += spaceNeeded / StoreConfig::BLOCK_LENGTH * overheadPerBlock;
            numBlocks = size_t(float(spaceNeeded) / fillDegree) / StoreConfig::BLOCK_LENGTH;
            blocks.resize(numBlocks);
            constructionTimer.notifyDeterminedSpace();

            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                StoreConfig::length_t size = objectProvider.getLength(key);
                insert(key, size);
                LOG("Inserting", i, numObjects);
            }
            constructionTimer.notifyPlacedObjects();
            BlockObjectWriter::writeBlocks(filename, blocks, objectProvider);
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            LOG("Looking up file size");
            size_t fileSize = readSpecialObject0(filename) * StoreConfig::BLOCK_LENGTH;
            numBlocks = (fileSize + StoreConfig::BLOCK_LENGTH - 1) / StoreConfig::BLOCK_LENGTH;
            LOG(nullptr);
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return 0;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: O(1)"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return 2 * StoreConfig::BLOCK_LENGTH;
        }

        size_t requiredIosPerQuery() override {
            return 2;
        }

    private:
        void insert(StoreConfig::key_t key, StoreConfig::length_t length) {
            Item item{key, length, 0};
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t block = fastrange64(MurmurHash64Seeded(item.key, item.userData), numBlocks);
                blocks.at(block).items.push_back(item);
                blocks.at(block).length += item.length + overheadPerObject;

                size_t maxSize = StoreConfig::BLOCK_LENGTH - overheadPerBlock;
                if (block == 0) {
                    maxSize -= overheadPerObject + sizeof(MetadataObjectType);
                }
                while (blocks.at(block).length > maxSize) {
                    size_t bumpedItemIndex = rand() % blocks.at(block).items.size();
                    Item bumpedItem = blocks.at(block).items.at(bumpedItemIndex);
                    bumpedItem.userData = (bumpedItem.userData + 1) % 2;
                    blocks.at(block).items.erase(blocks.at(block).items.begin() + bumpedItemIndex);
                    blocks.at(block).length -= bumpedItem.length + overheadPerObject;
                    insertionQueue.push_back(bumpedItem);
                }
            }
        }

    public:
        template <typename IoManager>
        void submitSingleQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            handle->state = 1;
            handle->stats.notifyStartQuery();
            size_t blockIndex1 = fastrange64(MurmurHash64Seeded(handle->key, 0), numBlocks);
            size_t blockIndex2 = fastrange64(MurmurHash64Seeded(handle->key, 1), numBlocks);
            handle->stats.notifyFoundBlock(2);
            ioManager->enqueueRead(handle->buffer, blockIndex1 * StoreConfig::BLOCK_LENGTH, StoreConfig::BLOCK_LENGTH,
                                   reinterpret_cast<uint64_t>(handle));
            ioManager->enqueueRead(handle->buffer + StoreConfig::BLOCK_LENGTH, blockIndex2 * StoreConfig::BLOCK_LENGTH, StoreConfig::BLOCK_LENGTH,
                                   reinterpret_cast<uint64_t>(handle));
        }

        template <typename IoManager>
        QueryHandle *peekAny(IoManager ioManager) {
            (void) ioManager;
            return nullptr;
        }

        template <typename IoManager>
        QueryHandle *awaitAny(IoManager ioManager) {
            QueryHandle *handle = reinterpret_cast<QueryHandle *>(ioManager->awaitAny());
            while (handle->state == 1) {
                // We just found the first block of a handle. Increment state and wait for another handle
                handle->state++;
                handle = reinterpret_cast<QueryHandle *>(ioManager->awaitAny());
            }
            handle->stats.notifyFetchedBlock();

            std::tuple<StoreConfig::length_t, char *> result = findKeyWithinBlock(handle->key, handle->buffer);
            if (std::get<1>(result) == nullptr) {
                result = findKeyWithinBlock(handle->key, handle->buffer + StoreConfig::BLOCK_LENGTH);
            }
            handle->length = std::get<0>(result);
            handle->resultPtr = std::get<1>(result);
            handle->stats.notifyFoundKey();
            handle->state = 0;
            return handle;
        }
};
