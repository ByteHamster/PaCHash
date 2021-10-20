#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>
#include <container/bit_vector.hpp>

#include "PageConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"

/**
 * Simple hash table that bumps cells that are overfilled.
 * Proof of concept without actual IO.
 */
class BumpingHashObjectStore : public VariableSizeObjectStore {
    private:
        using Super = VariableSizeObjectStore;
        using Item = typename Super::Item;
        pasta::BitVector *overflownBuckets = nullptr;
        pasta::BitVectorRank *rank = nullptr;
        BumpingHashObjectStore *nextLayer = nullptr;
        uint64_t hashSeed = 0;
        std::string childFileName;
    public:
        using QueryHandle = typename Super::QueryHandle;

        explicit BumpingHashObjectStore(float fillDegree, const char* filename)
                : VariableSizeObjectStore(fillDegree, filename) {
        }

        static std::string name() {
            return "BumpingHashObjectStore";
        }

        size_t hash(uint64_t key) {
            return fastrange64(MurmurHash64Seeded(key, hashSeed), this->numBuckets);
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
            numBuckets = std::max(numBuckets, 5ul);
            this->buckets.resize(this->numBuckets);
            constructionTimer.notifyDeterminedSpace();

            for (int i = 0; i < this->numObjects; i++) {
                uint64_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                size_t size = objectProvider.getLength(key);
                totalPayloadSize += size;
                Item item{key, size, 0};
                size_t bucket = hash(key);
                this->buckets.at(bucket).items.push_back(item);
                this->buckets.at(bucket).length += item.length + overheadPerObject;

                LOG("Inserting", i, this->numObjects);
            }
            size_t bitVectorSize = numBuckets;
            while ((((bitVectorSize>>6) + 1) & 7) != 0) { // Workaround for crash in pasta
                bitVectorSize += 64;
            }
            overflownBuckets = new pasta::BitVector(bitVectorSize, false);
            size_t overflown = 0;
            size_t maxSize = PageConfig::PAGE_SIZE - overheadPerPage;
            size_t writeTo = 0;
            std::vector<uint64_t> overflownKeys;
            for (int i = 0; i < numBuckets; i++) {
                if (buckets.at(i).length > maxSize) {
                    (*overflownBuckets)[i] = true;
                    overflown++;
                    for (Item &item : buckets.at(i).items) {
                        overflownKeys.push_back(item.key);
                    }
                } else {
                    buckets.at(writeTo) = buckets.at(i);
                    writeTo++;
                }
                LOG("Detecting overflowing buckets", i, numBuckets);
            }
            LOG("Building rank data structure");
            rank = new pasta::BitVectorRank(*overflownBuckets);
            buckets.resize(numBuckets - overflown);

            if (overflown > 0) {
                childFileName = filename;
                childFileName += "_";
                nextLayer = new BumpingHashObjectStore(fillDegree, childFileName.c_str());
                nextLayer->hashSeed = hashSeed + 1;
                nextLayer->writeToFile(overflownKeys, objectProvider);
            }

            constructionTimer.notifyPlacedObjects();
            //BucketObjectWriter::writeBuckets(filename, buckets, objectProvider);
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return 0;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: "<<prettyBytes(spaceUsage())<<std::endl;
            std::cout<<"Per block: "<<8.0*spaceUsage()/numBuckets<<std::endl;
            std::cout<<"External utilization over all levels: "<<100.0*numBuckets*fillDegree/totalActualBlocks()<<std::endl;
        }

        size_t spaceUsage() {
            return sizeof(*this) + rank->space_usage() + overflownBuckets->size()/8
                + ((nextLayer != nullptr) ? nextLayer->spaceUsage() : 0);
        }

        size_t totalActualBlocks() {
            return buckets.size() + ((nextLayer != nullptr) ? nextLayer->totalActualBlocks() : 0);
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: 1"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return 1 * PageConfig::PAGE_SIZE;
        }

        size_t requiredIosPerQuery() override {
            return 1;
        }

        std::queue<QueryHandle *> queryQueue;

        template <typename IoManager>
        void submitSingleQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            size_t bucket = hash(handle->key);
            if ((*overflownBuckets)[bucket]) {
                nextLayer->template submitSingleQuery(handle, ioManager);
            } else {
                handle->state = 1;
                handle->stats.notifyStartQuery();
                handle->stats.notifyFoundBlock();
            }
            queryQueue.push(handle);
        }

        template <typename IoManager>
        QueryHandle *peekAny(IoManager ioManager) {
            return nullptr;
        }

        template <typename IoManager>
        QueryHandle *awaitAny(IoManager ioManager) {
            assert(!queryQueue.empty());
            QueryHandle *handle = queryQueue.front();
            queryQueue.pop();

            size_t bucket = hash(handle->key);
            if ((*overflownBuckets)[bucket]) {
                return nextLayer->template awaitAny(ioManager);
            }

            handle->stats.notifyFetchedBlock();
            handle->stats.notifyFoundKey();
            handle->state = 0;

            size_t rankedBucket = rank->rank0(bucket);
            for (Item &item : buckets.at(rankedBucket).items) {
                if (item.key == handle->key) {
                    handle->length = item.length;
                    handle->resultPtr = reinterpret_cast<char *>(42);
                    return handle;
                }
            }
            return handle;
        }
};
