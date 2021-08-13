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
template <class Config = VariableSizeObjectStoreConfig>
class ParallelCuckooObjectStore : public FixedBlockObjectStore<Config> {
    private:
        using Super = FixedBlockObjectStore<Config>;
        using Item = typename Super::Item;
        size_t totalPayloadSize = 0;
        std::vector<Item> insertionQueue;
    public:
        using QueryHandle = typename Super::QueryHandle;

        explicit ParallelCuckooObjectStore(float fillDegree, const char* filename)
                : FixedBlockObjectStore<Config>(fillDegree, filename) {
        }

        virtual void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
            std::cout<<"Constructing ParallelCuckooObjectStore<"<<Config::IoManager::NAME()
                <<"> with alpha="<<this->fillDegree<<", N="<<this->numObjects<<std::endl;
            FixedBlockObjectStore<Config>::writeToFile(keys, objectProvider);

            for (int i = 0; i < this->numObjects; i++) {
                uint64_t key = keys.at(i);
                size_t size = objectProvider.getLength(key);
                totalPayloadSize += size;
                insert(key, size);
                this->LOG("Inserting", i, this->numObjects);
            }

            this->writeBuckets(objectProvider);
        }

        void reloadFromFile() final {
            this->LOG("Looking up file size");
            int fd = open(this->filename, O_RDONLY);
            struct stat fileStat = {};
            fstat(fd, &fileStat);
            this->numBuckets = (fileStat.st_size + PageConfig::PAGE_SIZE - 1) / PageConfig::PAGE_SIZE;
            close(fd);
            this->LOG(nullptr);
        }

        void printConstructionStats() final {
            std::cout<<"External space usage: "<<prettyBytes(this->numBuckets*PageConfig::PAGE_SIZE)<<" ("
                <<(double)100*(totalPayloadSize + this->numObjects*sizeof(ObjectHeader))/(this->numBuckets*PageConfig::PAGE_SIZE)<<"% utilization)"<<std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/this->numObjects<<std::endl;
            std::cout<<"RAM space usage: O(1)"<<std::endl;
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<2<<" (parallel)"<<std::endl;
        }

        QueryHandle newQueryHandle(size_t batchSize) final {
            QueryHandle handle = Super::newQueryHandle(batchSize);
            handle.ioManager = std::make_unique<typename Config::IoManager>(2 * batchSize, PageConfig::PAGE_SIZE, this->filename);
            return handle;
        }

    private:
        void insert(uint64_t key, size_t length) {
            insert({key, length, 0});
        }

        void insert(Item item) {
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t bucket = fastrange64(MurmurHash64Seeded(item.key, item.currentHashFunction), this->numBuckets);
                this->buckets.at(bucket).items.push_back(item);
                this->buckets.at(bucket).length += item.length + sizeof(ObjectHeader);

                while (this->buckets.at(bucket).length > PageConfig::PAGE_SIZE) {
                    size_t bumpedItemIndex = rand() % this->buckets.at(bucket).items.size();
                    Item bumpedItem = this->buckets.at(bucket).items.at(bumpedItemIndex);
                    bumpedItem.currentHashFunction = (bumpedItem.currentHashFunction + 1) % 2;
                    this->buckets.at(bucket).items.erase(this->buckets.at(bucket).items.begin() + bumpedItemIndex);
                    this->buckets.at(bucket).length -= bumpedItem.length + sizeof(ObjectHeader);
                    insertionQueue.push_back(bumpedItem);
                }
            }
        }

    protected:
        void submitQuery(QueryHandle &handle) final {
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
            handle.ioManager->awaitCompletion();
            handle.stats.notifyFetchedBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                char *blockContent1 = handle.resultPointers.at(i);
                char *blockContent2 = reinterpret_cast<char *>(handle.resultLengths.at(i));

                std::tuple<size_t, char *> result = this->findKeyWithinBlock(handle.keys.at(i), blockContent1);
                if (std::get<1>(result) == nullptr) {
                    result = this->findKeyWithinBlock(handle.keys.at(i), blockContent2);
                }
                handle.resultLengths.at(i) = std::get<0>(result);
                handle.resultPointers.at(i) = std::get<1>(result);
            }
            handle.stats.notifyFoundKey();
        }
};
