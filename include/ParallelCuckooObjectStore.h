#pragma once

#include <vector>

#include "StoreConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"
#include "BlockObjectWriter.h"

namespace pachash {
/**
 * Simple cuckoo hash table that loads both possible locations in parallel.
 */
class ParallelCuckooObjectStore : public VariableSizeObjectStore {
    private:
        using Super = VariableSizeObjectStore;
        using Item = typename BlockObjectWriter::Item;
        using Block = typename BlockObjectWriter::Block;
        std::vector<Block> blocks;
        std::vector<Item> insertionQueue;
    public:
        explicit ParallelCuckooObjectStore(float loadFactor, const char* filename, int openFlags)
                : VariableSizeObjectStore(loadFactor, filename, openFlags) {
        }

        static std::string name() {
            return "ParallelCuckooObjectStore";
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
            numObjects = end-begin;
            maxSize = 0;
            size_t spaceNeeded = 0;
            Iterator it = begin;
            while (it != end) {
                size_t length = lengthExtractor(*it);
                spaceNeeded += length;
                maxSize = std::max(maxSize, length);
                it++;
            }
            totalPayloadSize = spaceNeeded;
            spaceNeeded += numObjects * overheadPerObject;
            spaceNeeded += spaceNeeded / StoreConfig::BLOCK_LENGTH * overheadPerBlock;
            numBlocks = size_t(float(spaceNeeded) / loadFactor) / StoreConfig::BLOCK_LENGTH;
            blocks.resize(numBlocks);
            constructionTimer.notifyDeterminedSpace();

            it = begin;
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = hashFunction(*it);
                assert(key != 0); // Key 0 holds metadata
                size_t size = lengthExtractor(*it);
                insert(key, size, &*it);
                LOG("Inserting", i, numObjects);
                it++;
            }
            constructionTimer.notifyPlacedObjects();
            BlockObjectWriter::writeBlocks<ValuePointerExtractor, U>(
                    filename, openFlags, maxSize, blocks,
                    valuePointerExtractor, VariableSizeObjectStore::StoreMetadata::TYPE_CUCKOO);
            constructionTimer.notifyWroteObjects();
            blocks.clear();
            blocks.shrink_to_fit();
        }

        void writeToFile(std::vector<std::pair<std::string, std::string>> &vector) {
            auto hashFunction = [](const std::pair<std::string, std::string> &x) -> StoreConfig::key_t {
                return bytehamster::util::MurmurHash64(std::get<0>(x).data(), std::get<0>(x).length());
            };
            auto lengthEx = [](const std::pair<std::string, std::string> &x) -> size_t {
                return std::get<1>(x).length();
            };
            auto valueEx = [](const std::pair<std::string, std::string> &x) -> const char * {
                return std::get<1>(x).data();
            };
            writeToFile(vector.begin(), vector.end(), hashFunction, lengthEx, valueEx);
        }

        void buildIndex() final {
            constructionTimer.notifySyncedFile();
            LOG("Looking up file size");
            StoreMetadata metadata = readMetadata(filename);
            if (metadata.type != StoreMetadata::TYPE_CUCKOO) {
                throw std::logic_error("Opened file of wrong type");
            }
            numBlocks = metadata.numBlocks;
            maxSize = metadata.maxSize;
            LOG(nullptr);
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return 0;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: O(1)"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return 2 * StoreConfig::BLOCK_LENGTH;
        }

        size_t requiredIosPerQuery() override {
            return 2;
        }

    private:
        void insert(StoreConfig::key_t key, size_t length, void *originalObject) {
            Item item{key, length, 0, 0, originalObject};
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t block = bytehamster::util::fastrange64(
                    bytehamster::util::MurmurHash64Seeded(item.key, item.hashFunctionIndex % 2), numBlocks);
                blocks.at(block).items.push_back(item);
                blocks.at(block).length += item.length + overheadPerObject;

                size_t maxSize = StoreConfig::BLOCK_LENGTH - overheadPerBlock;
                if (block == 0) {
                    maxSize -= overheadPerObject + sizeof(VariableSizeObjectStore::StoreMetadata);
                }
                while (blocks.at(block).length > maxSize) {
                    size_t bumpedItemIndex = rand() % blocks.at(block).items.size();
                    auto it = blocks.at(block).items.begin();
                    std::advance(it, bumpedItemIndex);
                    Item bumpedItem = *it;
                    bumpedItem.hashFunctionIndex++;
                    if (item.hashFunctionIndex > 100) {
                        // Empirically, making this number larger does not increase the success probability
                        // but increases the duration of failed construction attempts significantly.
                        throw std::invalid_argument("Unable to insert item. Try reducing the load factor.");
                    }

                    blocks.at(block).items.erase(it);
                    blocks.at(block).length -= bumpedItem.length + overheadPerObject;
                    insertionQueue.push_back(bumpedItem);
                }
            }
        }

    public:
        template <typename IoManager>
        void enqueueQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                throw std::logic_error("Used handle that did not go through awaitCompletion()");
            }
            handle->state = 1;
            handle->stats.notifyStartQuery();
            size_t blockIndex1 = bytehamster::util::fastrange64(
                bytehamster::util::MurmurHash64Seeded(handle->key, 0), numBlocks);
            size_t blockIndex2 = bytehamster::util::fastrange64(
                bytehamster::util::MurmurHash64Seeded(handle->key, 1), numBlocks);
            handle->stats.notifyFoundBlock(2);
            ioManager->enqueueRead(handle->buffer, blockIndex1 * StoreConfig::BLOCK_LENGTH, StoreConfig::BLOCK_LENGTH,
                                   reinterpret_cast<uint64_t>(handle));
            ioManager->enqueueRead(handle->buffer + StoreConfig::BLOCK_LENGTH,
                                   blockIndex2 * StoreConfig::BLOCK_LENGTH, StoreConfig::BLOCK_LENGTH,
                                   reinterpret_cast<uint64_t>(handle));
        }

        template <typename IoManager>
        QueryHandle *peekAny(IoManager ioManager) {
            (void) ioManager;
            return nullptr;
        }

        template <typename IoManager>
        QueryHandle *awaitAny(IoManager ioManager) {
            QueryHandle *handle = reinterpret_cast<QueryHandle *>(ioManager->awaitAny());
            while (handle->state == 1) {
                // We just found the first block of a handle. Increment state and wait for another handle
                handle->state++;
                handle = reinterpret_cast<QueryHandle *>(ioManager->awaitAny());
            }
            handle->stats.notifyFetchedBlock();

            std::tuple<size_t, char *> result
                    = findKeyWithinNonOverlappingBlock(handle->key, handle->buffer);
            if (std::get<1>(result) == nullptr) {
                result = findKeyWithinNonOverlappingBlock(handle->key, handle->buffer + StoreConfig::BLOCK_LENGTH);
            }
            handle->length = std::get<0>(result);
            handle->resultPtr = std::get<1>(result);
            handle->stats.notifyFoundKey();
            handle->state = 0;
            return handle;
        }
};

} // Namespace pachash
