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
class ParallelCuckooHashing : public FixedBlockObjectStore<Config> {
    private:
        using Item = typename FixedBlockObjectStore<Config>::Item;
        QueryTimer queryTimer;
        size_t totalPayloadSize = 0;
        std::vector<Item> insertionQueue;
    public:
        explicit ParallelCuckooHashing(float fillDegree, const char* filename)
                : FixedBlockObjectStore<Config>(fillDegree, filename) {
        }

        virtual void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
            std::cout<<"Constructing ParallelCuckooHashing<"<<Config::IoManager::NAME()
                <<"> with alpha="<<this->fillDegree<<", N="<<this->numObjects<<std::endl;
            FixedBlockObjectStore<Config>::writeToFile(keys, objectProvider);

            std::default_random_engine generator(std::random_device{}());
            std::uniform_int_distribution<uint64_t> uniformDist(0, UINT64_MAX);

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
            // Nothing to do: This method has O(1) internal space
            this->LOG(nullptr);
        }

        void printConstructionStats() final {
            std::cout<<"External space usage: "<<prettyBytes(this->numBuckets*PageConfig::PAGE_SIZE)<<" ("
                <<(double)100*(totalPayloadSize + this->numObjects*sizeof(ObjectHeader))/(this->numBuckets*PageConfig::PAGE_SIZE)<<"% utilization)"<<std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/this->numObjects<<std::endl;
            std::cout<<"RAM space usage: O(1)"<<std::endl;
        }

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

        void submitQuery(QueryHandle &handle) final {
            assert(handle.keys.size() <= PageConfig::MAX_SIMULTANEOUS_QUERIES);
            size_t bucketIndexes[2 * handle.keys.size()];
            queryTimer.notifyStartQuery(handle.keys.size());
            for (int i = 0; i < handle.keys.size(); i++) {
                bucketIndexes[2 * i + 0] = fastrange64(MurmurHash64Seeded(handle.keys.at(i), 0), this->numBuckets);
                bucketIndexes[2 * i + 1] = fastrange64(MurmurHash64Seeded(handle.keys.at(i), 1), this->numBuckets);
            }
            queryTimer.notifyFoundBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                // Abusing handle fields to store temporary data
                handle.resultPointers.at(i) = this->ioManagers.at(handle.handleId)->enqueueRead(bucketIndexes[2 * i + 0] * PageConfig::PAGE_SIZE,
                       PageConfig::PAGE_SIZE, this->pageReadBuffers.at(handle.handleId) + (2*i + 0) * PageConfig::PAGE_SIZE);
                handle.resultLengths.at(i) = (size_t) this->ioManagers.at(handle.handleId)->enqueueRead(bucketIndexes[2 * i + 1] * PageConfig::PAGE_SIZE,
                       PageConfig::PAGE_SIZE, this->pageReadBuffers.at(handle.handleId) + (2*i + 1) * PageConfig::PAGE_SIZE);
            }
            this->ioManagers.at(handle.handleId)->submit();
        }

        void awaitCompletion(QueryHandle &handle) final {
            this->ioManagers.at(handle.handleId)->awaitCompletion();
            queryTimer.notifyFetchedBlock();
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
            queryTimer.notifyFoundKey();
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<2<<" (parallel)"<<std::endl;
            queryTimer.print();
        }
};
