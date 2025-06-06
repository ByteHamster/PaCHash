#pragma once

#include <vector>
#include <random>
#include <pasta/bit_vector/bit_vector.hpp>
#include <pasta/bit_vector/support/flat_rank.hpp>

#include "StoreConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"

namespace pachash {
/**
 * Simple hash table that bumps cells that are overfilled.
 * Proof of concept without actual IO.
 */
class BumpingHashObjectStore : public VariableSizeObjectStore {
    private:
        using Super = VariableSizeObjectStore;
        using Item = typename BlockObjectWriter::Item;
        using Block = typename BlockObjectWriter::Block;
        std::vector<Block> blocks;
        pasta::BitVector *overflownBlocks = nullptr;
        pasta::FlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES> *rank = nullptr;
        BumpingHashObjectStore *nextLayer = nullptr;
        uint64_t hashSeed = 0;
        std::string childFileName;
        size_t cutoff;
    public:

        explicit BumpingHashObjectStore(float loadFactor, const char* filename, int openFlags, size_t cutoff = ~0uL)
                : VariableSizeObjectStore(loadFactor, filename, openFlags), cutoff(cutoff) {
        }

        static std::string name() {
            return "BumpingHashObjectStore";
        }

        StoreConfig::key_t hash(StoreConfig::key_t key) {
            return bytehamster::util::fastrange64(bytehamster::util::MurmurHash64Seeded(key, hashSeed), numBlocks);
        }

        template <class Iterator, typename HashFunction, typename LengthExtractor, typename ValuePointerExtractor,
                class U = typename std::iterator_traits<Iterator>::value_type>
        void writeToFile(Iterator begin, Iterator end, HashFunction hashFunction,
                         LengthExtractor lengthExtractor, ValuePointerExtractor valuePointerExtractor) {
            constructionTimer.notifyStartConstruction();
            LOG("Calculating total size to determine number of blocks");
            numObjects = end - begin;
            maxSize = 0;
            size_t spaceNeeded = 0;
            Iterator it = begin;
            while (it != end) {
                size_t length = lengthExtractor(*it);
                spaceNeeded += length;
                maxSize = std::max(maxSize, length);
                it++;
            }
            spaceNeeded += numObjects * overheadPerObject;
            spaceNeeded += spaceNeeded / StoreConfig::BLOCK_LENGTH * overheadPerBlock;
            numBlocks = size_t(float(spaceNeeded) / loadFactor) / StoreConfig::BLOCK_LENGTH;
            // Store 1% of the objects in the last layer, which is 3 times as large.
            // This increases the theoretical space by 3% but decreases overhead from having too many small layers.
            if (cutoff == ~0uL) {
                cutoff = numObjects / 100;
            }
            if (numObjects <= cutoff || numBlocks < 500) {
                // For fewer than 500 blocks, the "this" object size is larger than the bit array size.
                numBlocks = std::max(3 * numBlocks, 500ul);
            }
            blocks.resize(this->numBlocks);
            constructionTimer.notifyDeterminedSpace();

            it = begin;
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = hashFunction(*it);
                assert(key != 0); // Key 0 holds metadata
                size_t size = lengthExtractor(*it);
                totalPayloadSize += size;
                Item item{key, size, 0, 0, &*it};
                size_t block = hash(key);
                this->blocks.at(block).items.push_back(item);
                this->blocks.at(block).length += item.length + overheadPerObject;

                LOG("Inserting", i, this->numObjects);
                it++;
            }
            overflownBlocks = new pasta::BitVector(numBlocks, false);
            size_t overflown = 0;
            size_t maxSize = StoreConfig::BLOCK_LENGTH - overheadPerBlock;
            size_t writeTo = 0;
            std::vector<U> overflownKeys;
            for (size_t i = 0; i < numBlocks; i++) {
                if (blocks.at(i).length > maxSize) {
                    (*overflownBlocks)[i] = true;
                    overflown++;
                    for (const Item &item : blocks.at(i).items) {
                        overflownKeys.push_back(hashFunction(*((U*)item.ptr)));
                    }
                } else {
                    blocks.at(writeTo) = blocks.at(i);
                    writeTo++;
                }
                LOG("Detecting overflowing blocks", i, numBlocks);
            }
            LOG("Building rank data structure");
            rank = new pasta::FlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES>(*overflownBlocks);
            blocks.resize(numBlocks - overflown);

            if (overflown > 0) {
                childFileName = filename;
                childFileName += "_";
                nextLayer = new BumpingHashObjectStore(loadFactor, childFileName.c_str(), openFlags, cutoff);
                nextLayer->hashSeed = hashSeed + 1;
                nextLayer->writeToFile(overflownKeys.begin(), overflownKeys.end(),
                                       hashFunction, lengthExtractor, valuePointerExtractor);
            }

            constructionTimer.notifyPlacedObjects();
            BlockObjectWriter::writeBlocks<ValuePointerExtractor, U>(
                    filename, openFlags, maxSize, blocks, valuePointerExtractor, 42);
            constructionTimer.notifyWroteObjects();
        }

        ~BumpingHashObjectStore() override {
            delete rank;
            delete nextLayer;
        }

        void buildIndex() final {
            constructionTimer.notifySyncedFile();
            constructionTimer.notifyReadComplete();
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout << "RAM space usage: "<<bytehamster::util::prettyBytes(totalSpaceUsage())<<std::endl;
            std::cout << "External utilization over all levels: "
                    << 100.0 * totalPayloadSize / (totalActualBlocks() * StoreConfig::BLOCK_LENGTH) << std::endl;
            printStats(numBlocks);
        }

        void printStats(size_t totalBlocks) {
            std::cout<<"Layer:"<<std::endl;
            std::cout<<"  Objects: "<<numObjects<<std::endl;
            std::cout<<"  Space: "<<bytehamster::util::prettyBytes(spaceThisLayer())<<std::endl;
            std::cout<<"  Per global block: "<<8.0*spaceThisLayer()/totalBlocks<<std::endl;
            std::cout<<"  External blocks: "<<blocks.size()<<std::endl;
            std::cout<<"  Bumped: "<<100 * (1.0 - (float)blocks.size()/(float)numBlocks)<<"%"<<std::endl;
            if (nextLayer != nullptr) {
                nextLayer->printStats(totalBlocks);
            }
        }

        size_t spaceThisLayer() {
            return sizeof(*this) + rank->space_usage() + overflownBlocks->size() / 8;
        }

        /**
         * In bytes
         */
        size_t totalSpaceUsage() {
            return spaceThisLayer() + ((nextLayer != nullptr) ? nextLayer->totalSpaceUsage() : 0);
        }

        float internalSpaceUsage() final {
            return (float) totalSpaceUsage() * 8.0f / (float) numBlocks;
        }

        size_t totalActualBlocks() {
            return blocks.size() + ((nextLayer != nullptr) ? nextLayer->totalActualBlocks() : 0);
        }

        size_t requiredBufferPerQuery() override {
            return 1 * StoreConfig::BLOCK_LENGTH;
        }

        size_t requiredIosPerQuery() override {
            return 1;
        }

        std::queue<QueryHandle *> queryQueue;

        template <typename IoManager>
        void enqueueQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                throw std::logic_error("Used handle that did not go through awaitCompletion()");
            }
            size_t block = hash(handle->key);
            if ((*overflownBlocks)[block]) {
                nextLayer->template enqueueQuery(handle, ioManager);
            } else {
                handle->state = 1;
                handle->stats.notifyStartQuery();
                handle->stats.notifyFoundBlock(1);
            }
            queryQueue.push(handle);
        }

        template <typename IoManager>
        QueryHandle *peekAny(IoManager ioManager) {
            (void) ioManager;
            return nullptr;
        }

        /**
         * The BumpingHash data structure is just a proof-of-concept. Retrieving objects is not supported.
         */
        template <typename IoManager>
        QueryHandle *awaitAny(IoManager ioManager) {
            assert(!queryQueue.empty());
            QueryHandle *handle = queryQueue.front();
            queryQueue.pop();

            size_t block = hash(handle->key);
            if ((*overflownBlocks)[block]) {
                return nextLayer->template awaitAny(ioManager);
            }

            handle->stats.notifyFetchedBlock();
            handle->stats.notifyFoundKey();
            handle->state = 0;

            size_t rankedBlock = rank->rank0(block);
            for (const Item &item : blocks.at(rankedBlock).items) {
                if (item.key == handle->key) {
                    handle->length = item.length;
                    handle->resultPtr = reinterpret_cast<char *>(42);
                    return handle;
                }
            }
            return handle;
        }
};

} // Namespace pachash
