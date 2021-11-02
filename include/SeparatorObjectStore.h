#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "StoreConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"
#include "BlockObjectWriter.h"
#include "BlockIterator.h"

#define INCREMENTAL_INSERT

/**
 * For each block, store a separator hash that determines whether an element is stored in the block or must
 * continue looking through its probe sequence.
 * See: "File organization: Implementation of a method guaranteeing retrieval in one access" (Larson, Kajla)
 */
template <size_t separatorBits = 6>
class SeparatorObjectStore : public VariableSizeObjectStore {
    private:
        using Super = VariableSizeObjectStore;
        using Item = typename Super::Item;
        size_t numQueries = 0;
        size_t numInternalProbes = 0;
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

        void writeToFile(std::vector<StoreConfig::key_t> &keys, ObjectProvider &objectProvider) final {
            constructionTimer.notifyStartConstruction();
            LOG("Calculating total size to determine number of blocks");
            numObjects = keys.size();
            size_t spaceNeeded = 0;
            for (StoreConfig::key_t key : keys) {
                spaceNeeded += objectProvider.getLength(key);
            }
            spaceNeeded += keys.size() * overheadPerObject;
            spaceNeeded += spaceNeeded / StoreConfig::BLOCK_LENGTH * overheadPerBlock;
            numBlocks = (spaceNeeded / fillDegree) / StoreConfig::BLOCK_LENGTH;
            blocks.resize(numBlocks);
            constructionTimer.notifyDeterminedSpace();

            separators = sdsl::int_vector<separatorBits>(numBlocks, (1 << separatorBits) - 1);
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                StoreConfig::length_t size = objectProvider.getLength(key);
                totalPayloadSize += size;

                #ifdef INCREMENTAL_INSERT
                    insert(key, size);
                #else
                    size_t bucket = chainBlock(key, 0);
                    buckets.at(bucket).items.push_back(Item{key, size, 0});
                    buckets.at(bucket).length += size + overheadPerObject;
                #endif

                LOG("Inserting", i, numObjects);
            }

            #ifndef INCREMENTAL_INSERT
                LOG("Repairing");
                for (size_t i = 0; i < numBlocks; i++) {
                    if (blocks.at(i).length > BUCKET_SIZE) {
                        handleOverflowingBlock(i);
                    }
                }
                LOG("Handling insertion queue");
                handleInsertionQueue();
            #endif

            constructionTimer.notifyPlacedObjects();
            BlockObjectWriter::writeBlocks(filename, blocks, objectProvider);
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            numBlocks = readSpecialObject0(filename);

            UringDoubleBufferBlockIterator blockIterator(filename, numBlocks, 2500, openFlags);
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
            return (double)separatorBits/fillDegree;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: "
                     <<prettyBytes(separators.capacity()/8)<<" ("<<separatorBits<<" bits/block, scaled: "
                     <<internalSpaceUsage()<<" bits/block)"<<std::endl;
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
                    // We already bumped items from this block. We therefore cannot insert new ones with larger separator
                    item.userData++;
                    block = chainBlock(item.key, item.userData);

                    if (item.userData > 100) {
                        // Empirically, making this number larger does not increase the success probability
                        // but increases the duration of failed construction attempts significantly.
                        std::cout<<std::endl;
                        std::cerr<<"Unable to insert item. Please reduce the load factor and try again."<<std::endl;
                        exit(1);
                    }
                }

                blocks.at(block).items.push_back(item);
                blocks.at(block).length += item.length + overheadPerObject;

                size_t maxSize = StoreConfig::BLOCK_LENGTH - overheadPerBlock;
                if (block == 0) {
                    maxSize -= sizeof(MetadataObjectType) + overheadPerObject;
                }
                if (blocks.at(block).length > maxSize) {
                    handleOverflowingBucket(block);
                }
            }
        }

        void handleOverflowingBucket(size_t block) {
            size_t maxSize = StoreConfig::BLOCK_LENGTH - overheadPerBlock;
            if (block == 0) {
                maxSize -= sizeof(MetadataObjectType) + overheadPerObject;
            }
            if (blocks.at(block).length <= maxSize) {
                return;
            }

            /*ips2ra::sort(blocks.at(block).items.begin(), blocks.at(block).items.end(), [&](const Item &item) {
                return separator(item.key, block);
            });*/
            std::sort(blocks.at(block).items.begin(), blocks.at(block).items.end(),
                      [&]( const auto& lhs, const auto& rhs ) {
                          return separator(lhs.key, block) < separator(rhs.key, block);
                      });

            size_t sizeSum = 0;
            size_t i = 0;
            size_t tooLargeItemSeparator = ~0ul;
            for (;i < blocks.at(block).items.size(); i++) {
                sizeSum += blocks.at(block).items.at(i).length + overheadPerObject;
                if (sizeSum > maxSize) {
                    tooLargeItemSeparator = separator(blocks.at(block).items.at(i).key, block);
                    break;
                }
            }
            assert(tooLargeItemSeparator != ~0ul);

            sizeSum = 0;
            i = 0;
            for (;i < blocks.at(block).items.size(); i++) {
                if (separator(blocks.at(block).items.at(i).key, block) >= tooLargeItemSeparator) {
                    break;
                }
                sizeSum += blocks.at(block).items.at(i).length + overheadPerObject;
            }

            std::vector<Item> overflow(blocks.at(block).items.begin() + i, blocks.at(block).items.end());
            assert(blocks.at(block).items.size() == overflow.size() + i);
            assert(tooLargeItemSeparator != 0 || i == 0);

            blocks.at(block).items.resize(i);
            blocks.at(block).length = sizeSum;
            assert(separators[block] == 0 || tooLargeItemSeparator <= separators[block]);
            separators[block] = tooLargeItemSeparator;
            for (Item &overflowedItem : overflow) {
                overflowedItem.userData++;
                insertionQueue.push_back(overflowedItem);
            }
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
        void submitSingleQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
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
