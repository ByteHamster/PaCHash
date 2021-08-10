#ifndef TESTCOMPARISON_PARALLELCUCKOOHASHING_H
#define TESTCOMPARISON_PARALLELCUCKOOHASHING_H

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "Hash.h"
#include "PageConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"

template <class Config = VariableSizeObjectStoreConfig>
class ParallelCuckooHashing : public FixedBlockObjectStore<Config> {
    private:
        using Item = typename FixedBlockObjectStore<Config>::Item;
        QueryTimer queryTimer;
        size_t totalPayloadSize = 0;
        char *pageReadBuffer;
        std::vector<Item> insertionQueue;
        std::unique_ptr<typename Config::IoManager> ioManager = nullptr;
    public:
        explicit ParallelCuckooHashing(size_t numObjects, size_t averageSize, float fillDegree, const char* filename)
                : FixedBlockObjectStore<Config>(numObjects, averageSize, fillDegree, filename) {
            pageReadBuffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, PageConfig::MAX_SIMULTANEOUS_QUERIES * 2 * PageConfig::PAGE_SIZE * sizeof(char)));
            std::cout<<"Constructing ParallelCuckooHashing<"<<Config::IoManager::NAME()<<"> with alpha="<<fillDegree<<", N="<<(double)numObjects<<", L="<<averageSize<<std::endl;
        }

        ~ParallelCuckooHashing() {
            free(pageReadBuffer);
        }

        void generateInputData(std::vector<std::pair<uint64_t, size_t>> &keysAndLengths,
                               std::function<const char*(uint64_t)> valuePointer) final {
            this->buckets.resize(this->numBuckets);
            std::default_random_engine generator(std::random_device{}());
            std::uniform_int_distribution<uint64_t> uniformDist(0, UINT64_MAX);

            for (int i = 0; i < this->numObjects; i++) {
                uint64_t key = keysAndLengths.at(i).first;
                size_t size = keysAndLengths.at(i).second;
                totalPayloadSize += size;
                insert(key, size);
                this->LOG("Inserting", i, this->numObjects);
            }

            this->writeBuckets(valuePointer);
        }

        void reloadInputDataFromFile() final {
            // Nothing to do: This method has O(1) internal space
            this->LOG(nullptr);
            ioManager = std::make_unique<typename Config::IoManager>(this->filename);
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

                size_t bucket = Hash::hash(item.key, item.currentHashFunction, this->numBuckets);
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

        std::vector<std::tuple<size_t, char *>> query(std::vector<uint64_t> &keys) final {
            assert(keys.size() <= PageConfig::MAX_SIMULTANEOUS_QUERIES);
            size_t bucketIndexes[2 * keys.size()];
            queryTimer.notifyStartQuery(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                bucketIndexes[2 * i + 0] = Hash::hash(keys.at(i), 0, this->numBuckets);
                bucketIndexes[2 * i + 1] = Hash::hash(keys.at(i), 1, this->numBuckets);
            }
            queryTimer.notifyFoundBlock();
            char *blockContents[2 * keys.size()];
            for (int i = 0; i < 2 * keys.size(); i++) {
                blockContents[i] = ioManager->readBlocks(bucketIndexes[i] * PageConfig::PAGE_SIZE,
                         PageConfig::PAGE_SIZE, pageReadBuffer + i * PageConfig::PAGE_SIZE);
            }
            ioManager->awaitCompletionOfReadRequests();
            queryTimer.notifyFetchedBlock();
            std::vector<std::tuple<size_t, char *>> result(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                result.at(i) = this->findKeyWithinBlock(keys.at(i), blockContents[2 * i + 0]);
                if (std::get<1>(result.at(i)) == nullptr) {
                    result.at(i) = this->findKeyWithinBlock(keys.at(i), blockContents[2 * i + 1]);
                }
            }
            queryTimer.notifyFoundKey();
            return result;
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<2<<" (parallel)"<<std::endl;
            queryTimer.print();
        }
};

#endif // TESTCOMPARISON_PARALLELCUCKOOHASHING_H