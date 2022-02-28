#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "StoreConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"
#include "BlockObjectWriter.h"
#include "BlockIterator.h"

namespace pachash {
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
        using Block = typename BlockObjectWriter::Block;
        size_t numQueries = 0;
        size_t numInternalProbes = 0;
        std::vector<Block> blocks;
        std::vector<Item> insertionQueue;
        sdsl::int_vector<separatorBits> separators;
    public:
        explicit SeparatorObjectStore(float loadFactor, const char* filename, int openFlags)
                : VariableSizeObjectStore(loadFactor, filename, openFlags) {
        }

        static std::string name() {
            return "SeparatorObjectStore s=" + std::to_string(separatorBits);
        }

        template <class Iterator, typename HashFunction, typename LengthExtractor, typename ValuePointerExtractor,
                class U = typename std::iterator_traits<Iterator>::value_type>
        void writeToFile(Iterator begin, Iterator end, HashFunction hashFunction,
                         LengthExtractor lengthExtractor, ValuePointerExtractor valuePointerExtractor) {
            static_assert(std::is_invocable_r_v<StoreConfig::key_t, HashFunction, U>);
            static_assert(std::is_invocable_r_v<size_t, LengthExtractor, U>);
            static_assert(std::is_invocable_r_v<const char *, ValuePointerExtractor, U>);
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
            numBlocks = (spaceNeeded / loadFactor) / StoreConfig::BLOCK_LENGTH;
            blocks.resize(numBlocks);
            constructionTimer.notifyDeterminedSpace();

            separators = sdsl::int_vector<separatorBits>(numBlocks, (1 << separatorBits) - 1);
            it = begin;
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = hashFunction(*it);
                assert(key != 0); // Key 0 holds metadata
                size_t size = lengthExtractor(*it);
                totalPayloadSize += size;

                insert(key, size, &*it);
                LOG("Inserting", i, numObjects);
                it++;
            }

            constructionTimer.notifyPlacedObjects();
            BlockObjectWriter::writeBlocks<ValuePointerExtractor, U>(
                    filename, openFlags, maxSize, blocks,
                    valuePointerExtractor, VariableSizeObjectStore::StoreMetadata::TYPE_SEPARATOR + separatorBits);
            constructionTimer.notifyWroteObjects();
        }

        void writeToFile(std::vector<std::pair<std::string, std::string>> &vector) {
            auto hashFunction = [](const std::pair<std::string, std::string> &x) -> StoreConfig::key_t {
                return MurmurHash64(std::get<0>(x).data(), std::get<0>(x).length());
            };
            auto lengthEx = [](const std::pair<std::string, std::string> &x) -> size_t {
                return std::get<1>(x).length();
            };
            auto valueEx = [](const std::pair<std::string, std::string> &x) -> const char * {
                return std::get<1>(x).data();
            };
            writeToFile(vector.begin(), vector.end(), hashFunction, lengthEx, valueEx);
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            StoreMetadata metadata = readMetadata(filename);
            if (metadata.type != StoreMetadata::TYPE_SEPARATOR + separatorBits) {
                throw std::logic_error("Opened file of wrong type");
            }
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
            std::cout << "RAM space usage: "
                      << prettyBytes(separators.capacity()/8) << " (" << separatorBits << " bits/block, scaled: "
                      << separatorBits / loadFactor << " bits/block)" << std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return StoreConfig::BLOCK_LENGTH;
        }

        size_t requiredIosPerQuery() override {
            return 1;
        }

    private:
        void insert(StoreConfig::key_t key, size_t length, void *originalObject) {
            Item item = {key, length, 0, 0, originalObject};
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        uint64_t separator(StoreConfig::key_t key, size_t bucket) {
            return fastrange64(MurmurHash64Seeded(key, bucket), (1 << separatorBits) - 1);
        }

        uint64_t chainBlock(StoreConfig::key_t key, size_t index) {
            return fastrange64(MurmurHash64Seeded(key + 1, index), numBlocks);
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t block = chainBlock(item.key, item.hashFunctionIndex);
                size_t separatorCache;
                while ((separatorCache = separator(item.key, block)) >= separators[block]) {
                    // We already bumped items from this block. We cannot insert new ones with larger separator
                    item.hashFunctionIndex++;
                    block = chainBlock(item.key, item.hashFunctionIndex);

                    if (item.hashFunctionIndex > 100) {
                        // Empirically, making this number larger does not increase the success probability
                        // but increases the duration of failed construction attempts significantly.
                        throw std::invalid_argument("Unable to insert item."
                                "Try reducing the load factor or increasing the separator length.");
                    }
                }

                item.currentHash = separatorCache;
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
                          return lhs.currentHash < rhs.currentHash;
            });

            blocks.at(block).length = 0;
            size_t tooLargeItemSeparator = ~0ul;
            auto it = blocks.at(block).items.begin();
            while (it != blocks.at(block).items.end()) {
                blocks.at(block).length += (*it).length + overheadPerObject;
                if (blocks.at(block).length > maxSize) {
                    tooLargeItemSeparator = it->currentHash;
                    break;
                }
                ++it;
            }
            assert(tooLargeItemSeparator != ~0ul);
            bool removeFromBeginning = false;
            while (it->currentHash>= tooLargeItemSeparator) {
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
                overflowedItem.hashFunctionIndex++;
                insertionQueue.push_back(overflowedItem);
                it = blocks.at(block).items.erase(it);
            }
            assert(tooLargeItemSeparator != 0 || blocks.at(block).items.size() == 0);
            assert(separators[block] == 0 || tooLargeItemSeparator <= separators[block]);
            separators[block] = tooLargeItemSeparator;
        }

    public:
        // This method is for internal use only.
        // It is public only to enable micro-benchmarks for the index data structure.
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
            std::tuple<size_t, char *> result = findKeyWithinNonOverlappingBlock(handle->key, handle->buffer);
            handle->length = std::get<0>(result);
            handle->resultPtr = std::get<1>(result);
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};

} // Namespace pachash
