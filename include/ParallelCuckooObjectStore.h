#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "PageConfig.h"
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

        explicit ParallelCuckooObjectStore(float fillDegree, const char* filename)
                : VariableSizeObjectStore(fillDegree, filename) {
        }

        static std::string name() {
            return "ParallelCuckooObjectStore";
        }

        void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
            constructionTimer.notifyStartConstruction();
            LOG("Calculating total size to determine number of blocks");
            this->numObjects = keys.size();
            size_t spaceNeeded = 0;
            for (unsigned long key : keys) {
                spaceNeeded += objectProvider.getLength(key);
            }
            spaceNeeded += keys.size() * overheadPerObject;
            spaceNeeded += spaceNeeded/PageConfig::PAGE_SIZE*overheadPerPage;
            this->numBuckets = size_t(float(spaceNeeded) / fillDegree) / PageConfig::PAGE_SIZE;
            this->buckets.resize(this->numBuckets);
            constructionTimer.notifyDeterminedSpace();

            for (int i = 0; i < this->numObjects; i++) {
                uint64_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                size_t size = objectProvider.getLength(key);
                totalPayloadSize += size;
                insert(key, size);
                LOG("Inserting", i, this->numObjects);
            }
            constructionTimer.notifyPlacedObjects();
            this->writeBuckets(objectProvider, false);
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            LOG("Looking up file size");
            size_t fileSize = readSpecialObject0(filename) * PageConfig::PAGE_SIZE;
            this->numBuckets = (fileSize + PageConfig::PAGE_SIZE - 1) / PageConfig::PAGE_SIZE;
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

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<2<<" (parallel)"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return 2 * PageConfig::PAGE_SIZE;
        }

        size_t requiredIosPerQuery() override {
            return 2;
        }

    private:
        void insert(uint64_t key, size_t length) {
            Item item{key, length, 0};
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t bucket = fastrange64(MurmurHash64Seeded(item.key, item.userData), this->numBuckets);
                this->buckets.at(bucket).items.push_back(item);
                this->buckets.at(bucket).length += item.length + overheadPerObject;

                size_t maxSize = PageConfig::PAGE_SIZE - overheadPerPage;
                if (bucket == 0) {
                    maxSize -= overheadPerObject + sizeof(MetadataObjectType);
                }
                while (this->buckets.at(bucket).length > maxSize) {
                    size_t bumpedItemIndex = rand() % this->buckets.at(bucket).items.size();
                    Item bumpedItem = this->buckets.at(bucket).items.at(bumpedItemIndex);
                    bumpedItem.userData = (bumpedItem.userData + 1) % 2;
                    this->buckets.at(bucket).items.erase(this->buckets.at(bucket).items.begin() + bumpedItemIndex);
                    this->buckets.at(bucket).length -= bumpedItem.length + overheadPerObject;
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
            size_t bucketIndex1 = fastrange64(MurmurHash64Seeded(handle->key, 0), numBuckets);
            size_t bucketIndex2 = fastrange64(MurmurHash64Seeded(handle->key, 1), numBuckets);
            handle->stats.notifyFoundBlock();
            ioManager->enqueueRead(handle->buffer, bucketIndex1 * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE,
                                   reinterpret_cast<uint64_t>(handle));
            ioManager->enqueueRead(handle->buffer + PageConfig::PAGE_SIZE, bucketIndex2 * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE,
                                   reinterpret_cast<uint64_t>(handle));
        }

        template <typename IoManager>
        QueryHandle *peekAny(IoManager ioManager) {
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

            std::tuple<size_t, char *> result = findKeyWithinBlock(handle->key, handle->buffer);
            if (std::get<1>(result) == nullptr) {
                result = findKeyWithinBlock(handle->key, handle->buffer + PageConfig::PAGE_SIZE);
            }
            handle->length = std::get<0>(result);
            handle->resultPtr = std::get<1>(result);
            handle->stats.notifyFoundKey();
            handle->state = 0;
            return handle;
        }
};
