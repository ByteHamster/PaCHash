#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "StoreConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"
#include "BlockObjectWriter.h"
#include "BlockIterator.h"

/**
 * For each block, store a separator hash that determines whether an element is stored in the block or must
 * continue looking through its probe sequence.
 * See: "File organization: Implementation of a method guaranteeing retrieval in one access" (Larson, Kajla)
 */
template <size_t separatorBits = 6>
class SeparatorObjectStore : public VariableSizeObjectStore {
    private:
        using Super = VariableSizeObjectStore;
        using Item = typename BlockObjectWriter::Item;
        using Block = typename BlockObjectWriter::SimpleBlock;
        size_t numQueries = 0;
        size_t numInternalProbes = 0;
        std::vector<Block> blocks;
        std::vector<Item> insertionQueue;
        sdsl::int_vector<separatorBits> separators;
    public:
        using QueryHandle = typename Super::QueryHandle;

        explicit SeparatorObjectStore(float fillDegree, const char* filename, int openFlags)
                : VariableSizeObjectStore(fillDegree, filename, openFlags) {
        }

        static std::string name() {
            return "SeparatorObjectStore s=" + std::to_string(separatorBits);
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
                StoreConfig::length_t length = lengthExtractor(*it);
                spaceNeeded += length;
                maxSize = std::max(maxSize, length);
                it++;
            }
            spaceNeeded += numObjects * overheadPerObject;
            spaceNeeded += spaceNeeded / StoreConfig::BLOCK_LENGTH * overheadPerBlock;
            numBlocks = (spaceNeeded / fillDegree) / StoreConfig::BLOCK_LENGTH;
            blocks.resize(numBlocks);
            constructionTimer.notifyDeterminedSpace();

            separators = sdsl::int_vector<separatorBits>(numBlocks, (1 << separatorBits) - 1);
            it = begin;
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = hashFunction(*it);
                assert(key != 0); // Key 0 holds metadata
                StoreConfig::length_t size = lengthExtractor(*it);
                totalPayloadSize += size;

                insert(key, size);
                LOG("Inserting", i, numObjects);
                it++;
            }

            constructionTimer.notifyPlacedObjects();
            BlockObjectWriter::writeBlocks(filename, openFlags, maxSize, blocks, valuePointerExtractor);
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            StoreMetadata metadata = readMetadata(filename);
            numBlocks = metadata.numBlocks;
            maxSize = metadata.maxSize;

            #ifdef HAS_LIBURING
            UringDoubleBufferBlockIterator blockIterator(filename, numBlocks, 2500, openFlags);
            #else
            PosixBlockIterator blockIterator(filename, 2500, openFlags);
            #endif
            size_t objectsFound = 0;
            separators = sdsl::int_vector<separatorBits>(numBlocks, 0);
            for (size_t blocksRead = 0; blocksRead < numBlocks; blocksRead++) {
                size_t blockIdx = blockIterator.blockNumber();
                BlockStorage block(blockIterator.blockContent());
                int maxSeparator = -1;
                for (size_t i = 0; i < block.numObjects; i++) {
                    if (block.keys[i] != 0) { // Key 0 holds metadata
                        maxSeparator = std::max(maxSeparator, (int) separator(block.keys[i], blockIdx));
                        objectsFound++;
                    }
                }
                separators[blockIdx] = maxSeparator + 1;
                if (blocksRead < numBlocks - 1) {
                    blockIterator.next();
                }
                LOG("Reading", blocksRead, numBlocks);
            }
            LOG(nullptr);
            numObjects = objectsFound;
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return (double)separatorBits;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: "
                     <<prettyBytes(separators.capacity()/8)<<" ("<<separatorBits<<" bits/block, scaled: "
                     <<separatorBits/fillDegree<<" bits/block)"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return StoreConfig::BLOCK_LENGTH;
        }

        size_t requiredIosPerQuery() override {
            return 1;
        }

    private:
        void insert(StoreConfig::key_t key, StoreConfig::length_t length) {
            insert({key, length, 0});
        }

        uint64_t separator(StoreConfig::key_t key, size_t bucket) {
            return fastrange64(MurmurHash64Seeded(key, bucket), (1 << separatorBits) - 1);
        }

        uint64_t chainBlock(StoreConfig::key_t key, size_t index) {
            return fastrange64(MurmurHash64Seeded(key + 1, index), numBlocks);
        }

        void insert(Item item) {
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t block = chainBlock(item.key, item.userData);
                while (separator(item.key, block) >= separators[block]) {
                    // We already bumped items from this block. We cannot insert new ones with larger separator
                    item.userData++;
                    block = chainBlock(item.key, item.userData);

                    if (item.userData > 100) {
                        // Empirically, making this number larger does not increase the success probability
                        // but increases the duration of failed construction attempts significantly.
                        throw std::invalid_argument("Unable to insert item. Try reducing the load factor.");
                    }
                }

                blocks.at(block).items.push_back(item);
                blocks.at(block).length += item.length + overheadPerObject;

                size_t maxSize = StoreConfig::BLOCK_LENGTH - overheadPerBlock;
                if (block == 0) {
                    maxSize -= sizeof(StoreMetadata) + overheadPerObject;
                }
                if (blocks.at(block).length > maxSize) {
                    handleOverflowingBucket(block);
                }
            }
        }

        void handleOverflowingBucket(size_t block) {
            size_t maxSize = StoreConfig::BLOCK_LENGTH - overheadPerBlock;
            if (block == 0) {
                maxSize -= sizeof(StoreMetadata) + overheadPerObject;
            }
            if (blocks.at(block).length <= maxSize) {
                return;
            }

            std::sort(blocks.at(block).items.begin(), blocks.at(block).items.end(),
                      [&]( const auto& lhs, const auto& rhs ) {
                          return separator(lhs.key, block) < separator(rhs.key, block);
                      });

            blocks.at(block).length = 0;
            size_t tooLargeItemSeparator = ~0ul;
            auto it = blocks.at(block).items.begin();
            while (it != blocks.at(block).items.end()) {
                blocks.at(block).length += (*it).length + overheadPerObject;
                if (blocks.at(block).length > maxSize) {
                    tooLargeItemSeparator = separator((*it).key, block);
                    break;
                }
                ++it;
            }
            assert(tooLargeItemSeparator != ~0ul);
            bool removeFromBeginning = false;
            while (separator((*it).key, block) >= tooLargeItemSeparator) {
                blocks.at(block).length -= (*it).length + overheadPerObject;
                if (it == blocks.at(block).items.begin()) {
                    removeFromBeginning = true;
                    break;
                }
                --it;
            }
            if (!removeFromBeginning) {
                ++it; // Iterator now points to first item that should be removed
            }

            while (it != blocks.at(block).items.end()) {
                Item overflowedItem = *it;
                overflowedItem.userData++;
                insertionQueue.push_back(overflowedItem);
                it = blocks.at(block).items.erase(it);
            }
            assert(tooLargeItemSeparator != 0 || blocks.at(block).items.size() == 0);
            assert(separators[block] == 0 || tooLargeItemSeparator <= separators[block]);
            separators[block] = tooLargeItemSeparator;
        }

        inline size_t findBlockToAccess(StoreConfig::key_t key) {
            for (size_t hashFunctionIndex = 0; hashFunctionIndex < 100000; hashFunctionIndex++) {
                size_t block = chainBlock(key, hashFunctionIndex);
                numInternalProbes++;
                if (separator(key, block) < static_cast<const sdsl::int_vector<separatorBits>&>(separators)[block]) {
                    return block;
                }
            }
            std::cerr<<"Unable to find block"<<std::endl;
            return -1;
        }

    public:
        template <typename IoManager>
        void enqueueQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                throw std::logic_error("Used handle that did not go through awaitCompletion()");
            }
            handle->state = 1;
            numQueries++;
            handle->stats.notifyStartQuery();
            size_t block = findBlockToAccess(handle->key);
            handle->stats.notifyFoundBlock(1);
            ioManager->enqueueRead(handle->buffer, block * StoreConfig::BLOCK_LENGTH, StoreConfig::BLOCK_LENGTH,
                                   reinterpret_cast<uint64_t>(handle));
        }

        template <typename IoManager>
        QueryHandle *peekAny(IoManager ioManager) {
            QueryHandle *handle = reinterpret_cast<QueryHandle *>(ioManager->peekAny());
            if (handle != nullptr) {
                parse(handle);
            }
            return handle;
        }

        template <typename IoManager>
        QueryHandle *awaitAny(IoManager ioManager) {
            QueryHandle *handle = reinterpret_cast<QueryHandle *>(ioManager->awaitAny());
            parse(handle);
            return handle;
        }

        void parse(QueryHandle *handle) {
            handle->stats.notifyFetchedBlock();
            std::tuple<StoreConfig::length_t, char *> result = findKeyWithinBlock(handle->key, handle->buffer);
            handle->length = std::get<0>(result);
            handle->resultPtr = std::get<1>(result);
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};
