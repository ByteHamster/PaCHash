#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>
#include <container/bit_vector.hpp>

#include "StoreConfig.h"
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
        pasta::BitVector *overflownBlocks = nullptr;
        pasta::BitVectorRank *rank = nullptr;
        BumpingHashObjectStore *nextLayer = nullptr;
        uint64_t hashSeed = 0;
        std::string childFileName;
    public:
        using QueryHandle = typename Super::QueryHandle;

        explicit BumpingHashObjectStore(float fillDegree, const char* filename, int openFlags)
                : VariableSizeObjectStore(fillDegree, filename, openFlags) {
        }

        static std::string name() {
            return "BumpingHashObjectStore";
        }

        StoreConfig::key_t hash(StoreConfig::key_t key) {
            return fastrange64(MurmurHash64Seeded(key, hashSeed), numBlocks);
        }

        template <class Iterator, typename HashFunction, typename LengthExtractor, typename ValuePointerExtractor,
                class U = typename std::iterator_traits<Iterator>::value_type>
        void writeToFile(Iterator begin, Iterator end, HashFunction hashFunction,
                         LengthExtractor lengthExtractor, ValuePointerExtractor valuePointerExtractor) {
            constructionTimer.notifyStartConstruction();
            LOG("Calculating total size to determine number of blocks");
            numObjects = end - begin;
            size_t spaceNeeded = 0;
            Iterator it = begin;
            while (it != end) {
                spaceNeeded += lengthExtractor(*it);
                it++;
            }
            spaceNeeded += numObjects * overheadPerObject;
            spaceNeeded += spaceNeeded / StoreConfig::BLOCK_LENGTH * overheadPerBlock;
            numBlocks = size_t(float(spaceNeeded) / fillDegree) / StoreConfig::BLOCK_LENGTH;
            numBlocks = std::max(numBlocks, 5ul);
            blocks.resize(this->numBlocks);
            constructionTimer.notifyDeterminedSpace();

            it = begin;
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = hashFunction(*it);
                assert(key != 0); // Key 0 holds metadata
                StoreConfig::length_t size = lengthExtractor(*it);
                totalPayloadSize += size;
                Item item{key, size, 0, &*it};
                size_t block = hash(key);
                this->blocks.at(block).items.push_back(item);
                this->blocks.at(block).length += item.length + overheadPerObject;

                LOG("Inserting", i, this->numObjects);
                it++;
            }
            size_t bitVectorSize = numBlocks;
            while ((((bitVectorSize>>6) + 1) & 7) != 0) { // Workaround for crash in pasta
                bitVectorSize += 64;
            }
            overflownBlocks = new pasta::BitVector(bitVectorSize, false);
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
            rank = new pasta::BitVectorRank(*overflownBlocks);
            blocks.resize(numBlocks - overflown);

            if (overflown > 0) {
                childFileName = filename;
                childFileName += "_";
                nextLayer = new BumpingHashObjectStore(fillDegree, childFileName.c_str(), openFlags);
                nextLayer->hashSeed = hashSeed + 1;
                nextLayer->writeToFile(overflownKeys.begin(), overflownKeys.end(), hashFunction, lengthExtractor, valuePointerExtractor);
            }

            constructionTimer.notifyPlacedObjects();
            BlockObjectWriter::writeBlocks(filename, blocks, valuePointerExtractor);
            constructionTimer.notifyWroteObjects();
        }

        virtual ~BumpingHashObjectStore() {
            delete rank;
            delete nextLayer;
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
            std::cout << "RAM space usage: "<<prettyBytes(spaceUsage())<<std::endl;
            std::cout << "Per block: " << 8.0 * spaceUsage() / numBlocks << std::endl;
            std::cout << "External utilization over all levels: " << 100.0 * numBlocks * fillDegree / totalActualBlocks() << std::endl;
        }

        size_t spaceUsage() {
            return sizeof(*this) + rank->space_usage() + overflownBlocks->size() / 8
                   + ((nextLayer != nullptr) ? nextLayer->spaceUsage() : 0);
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
        void submitSingleQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            size_t block = hash(handle->key);
            if ((*overflownBlocks)[block]) {
                nextLayer->template submitSingleQuery(handle, ioManager);
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
