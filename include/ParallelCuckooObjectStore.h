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

        /**
         * Create a new handle that can execute \p batchSize queries simultaneously.
         * Multiple handles can be used to execute multiple batches simultaneously.
         * It is advisable to batch queries instead of executing them one-by-one using a QueryHandle each.
         * Query handle creation is an expensive operation and should be done before the actual queries.
         * The returned object needs to be deleted by the caller.
         */
        template <typename IoManager = MemoryMapIO>
        QueryHandle *newQueryHandle(size_t batchSize, int openFlags = 0) {
            QueryHandle *handle = new QueryHandle(*this, batchSize);
            handle->ioManager = std::make_unique<IoManager>(openFlags, 2 * batchSize, PageConfig::PAGE_SIZE, this->filename);
            return handle;
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

                size_t bucket = fastrange64(MurmurHash64Seeded(item.key, item.currentHashFunction), this->numBuckets);
                this->buckets.at(bucket).items.push_back(item);
                this->buckets.at(bucket).length += item.length + overheadPerObject;

                size_t maxSize = PageConfig::PAGE_SIZE - overheadPerPage;
                if (bucket == 0) {
                    maxSize -= overheadPerObject + sizeof(MetadataObjectType);
                }
                while (this->buckets.at(bucket).length > maxSize) {
                    size_t bumpedItemIndex = rand() % this->buckets.at(bucket).items.size();
                    Item bumpedItem = this->buckets.at(bucket).items.at(bumpedItemIndex);
                    bumpedItem.currentHashFunction = (bumpedItem.currentHashFunction + 1) % 2;
                    this->buckets.at(bucket).items.erase(this->buckets.at(bucket).items.begin() + bumpedItemIndex);
                    this->buckets.at(bucket).length -= bumpedItem.length + overheadPerObject;
                    insertionQueue.push_back(bumpedItem);
                }
            }
        }

    protected:
        void submitQuery(QueryHandle &handle) final {
            if (!handle.completed) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            handle.completed = false;
            size_t bucketIndexes[2 * handle.keys.size()];
            handle.stats.notifyStartQuery(handle.keys.size());
            for (int i = 0; i < handle.keys.size(); i++) {
                bucketIndexes[2 * i + 0] = fastrange64(MurmurHash64Seeded(handle.keys.at(i), 0), this->numBuckets);
                bucketIndexes[2 * i + 1] = fastrange64(MurmurHash64Seeded(handle.keys.at(i), 1), this->numBuckets);
            }
            handle.stats.notifyFoundBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                // Abusing handle fields to store temporary data
                handle.resultPointers.at(i) = handle.ioManager->enqueueRead(
                        bucketIndexes[2 * i + 0] * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE);
                handle.resultLengths.at(i) = (size_t) handle.ioManager->enqueueRead(
                        bucketIndexes[2 * i + 1] * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE);
            }
            handle.ioManager->submit();
        }

        void awaitCompletion(QueryHandle &handle) final {
            if (handle.completed) {
                return;
            }
            handle.ioManager->awaitCompletion();
            handle.stats.notifyFetchedBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                char *blockContent1 = handle.resultPointers.at(i);
                char *blockContent2 = reinterpret_cast<char *>(handle.resultLengths.at(i));

                std::tuple<size_t, char *> result = findKeyWithinBlock(handle.keys.at(i), blockContent1);
                if (std::get<1>(result) == nullptr) {
                    result = findKeyWithinBlock(handle.keys.at(i), blockContent2);
                }
                handle.resultLengths.at(i) = std::get<0>(result);
                handle.resultPointers.at(i) = std::get<1>(result);
            }
            handle.stats.notifyFoundKey();
            handle.completed = true;
        }
};
