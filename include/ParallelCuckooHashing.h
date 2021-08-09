#ifndef TESTCOMPARISON_PARALLELCUCKOOHASHING_H
#define TESTCOMPARISON_PARALLELCUCKOOHASHING_H

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "Hash.h"
#include "PageConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"

template <class IoManager = MemoryMapIO<>>
class ParallelCuckooHashing : public FixedBlockObjectStore {
    private:
        QueryTimer queryTimer;
        size_t totalPayloadSize = 0;
        char *pageReadBuffer;
        std::vector<FixedBlockObjectStore::Item> insertionQueue;
        std::unique_ptr<IoManager> ioManager = nullptr;
    public:
        explicit ParallelCuckooHashing(size_t numObjects, size_t averageSize, float fillDegree)
                : FixedBlockObjectStore(numObjects, averageSize, fillDegree) {
            pageReadBuffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, PageConfig::MAX_SIMULTANEOUS_QUERIES * 2 * PageConfig::PAGE_SIZE * sizeof(char)));
            std::cout<<"Constructing ParallelCuckooHashing<"<<IoManager::NAME()<<"> with alpha="<<fillDegree<<", N="<<(double)numObjects<<", L="<<averageSize<<std::endl;
        }

        ~ParallelCuckooHashing() {
            free(pageReadBuffer);
        }

        void generateInputData() final {
            buckets.resize(numBuckets);
            std::default_random_engine generator(std::random_device{}());
            std::uniform_int_distribution<uint64_t> uniformDist(0, UINT64_MAX);

            std::vector<std::pair<uint64_t, size_t>> objects;
            objects.reserve(numObjects);

            std::cout<<"# Generating keys and lengths"<<std::flush;;
            for (int i = 0; i < numObjects; i++) {
                uint64_t key = uniformDist(generator);
                size_t size = Distribution::lengthFor<ParallelCuckooHashing::distribution>(key, ParallelCuckooHashing::averageSize);
                assert(size <= PageConfig::PAGE_SIZE);
                objects.emplace_back(key, size);
                keysTestingOnly.push_back(key);
            }

            for (int i = 0; i < numObjects; i++) {
                uint64_t key = objects.at(i).first;
                size_t size = objects.at(i).second;
                totalPayloadSize += size;
                insert(key, size);
                if (SHOW_PROGRESS && (i % (numObjects/PROGRESS_STEPS) == 0 || i == numObjects - 1)) {
                    std::cout<<"\r# Inserting "<<std::round(100.0*i/numObjects)<<"%"<<std::flush;
                }
            }

            writeBuckets();
        }

        void reloadInputDataFromFile() final {
            // Nothing to do: This method has O(1) internal space
            std::cout << "\r";
            ioManager = std::make_unique<IoManager>(INPUT_FILE);
        }

        void printConstructionStats() final {
            std::cout<<"External space usage: "<<prettyBytes(numBuckets*PageConfig::PAGE_SIZE)<<" ("
                     <<(double)100*(totalPayloadSize + numObjects*sizeof(ObjectHeader))/(numBuckets*PageConfig::PAGE_SIZE)<<"% utilization)"<<std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/numObjects<<std::endl;
            std::cout<<"RAM space usage: O(1)"<<std::endl;
        }

        void insert(uint64_t key, size_t length) {
            insert({key, length, 0});
        }

        void insert(FixedBlockObjectStore::Item item) {
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t bucket = Hash::hash(item.key, item.currentHashFunction, numBuckets);
                buckets.at(bucket).items.push_back(item);
                buckets.at(bucket).length += item.length + sizeof(ObjectHeader);

                while (buckets.at(bucket).length > PageConfig::PAGE_SIZE) {
                    size_t bumpedItemIndex = rand() % buckets.at(bucket).items.size();
                    Item bumpedItem = buckets.at(bucket).items.at(bumpedItemIndex);
                    bumpedItem.currentHashFunction = (bumpedItem.currentHashFunction + 1) % 2;
                    buckets.at(bucket).items.erase(buckets.at(bucket).items.begin() + bumpedItemIndex);
                    buckets.at(bucket).length -= bumpedItem.length + sizeof(ObjectHeader);
                    insertionQueue.push_back(bumpedItem);
                }
            }
        }

        std::vector<std::tuple<size_t, char *>> query(std::vector<uint64_t> &keys) final {
            assert(keys.size() <= PageConfig::MAX_SIMULTANEOUS_QUERIES);
            size_t bucketIndexes[2 * keys.size()];
            queryTimer.notifyStartQuery(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                bucketIndexes[2 * i + 0] = Hash::hash(keys.at(i), 0, numBuckets);
                bucketIndexes[2 * i + 1] = Hash::hash(keys.at(i), 1, numBuckets);
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
                result.at(i) = findKeyWithinBlock(keys.at(i), blockContents[2 * i + 0]);
                if (std::get<1>(result.at(i)) == nullptr) {
                    result.at(i) = findKeyWithinBlock(keys.at(i), blockContents[2 * i + 1]);
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