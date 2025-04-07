#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <cstdio>
#include <fcntl.h>
#include <ips2ra.hpp>
#include <bytehamster/util/Function.h>
#include <bytehamster/util/MurmurHash64.h>
#include <bytehamster/util/Files.h>
#include "IoManager.h"
#include "VariableSizeObjectStore.h"
#include "LinearObjectWriter.h"
#include "BlockIterator.h"
#include "PaCHashIndex.h"

namespace pachash {
/**
 * Store the first bin intersecting with each block with a predecessor data structure.
 * Execute a predecessor query to retrieve the key location.
 */
template <uint16_t a, typename Index = EliasFanoIndex<bytehamster::util::ceillog2(a)>>
class PaCHashObjectStore : public VariableSizeObjectStore {
    public:
        using Super = VariableSizeObjectStore;
        Index *index = nullptr;
        size_t numBins = 0;

        explicit PaCHashObjectStore([[maybe_unused]] float loadFactor, const char* filename, int openFlags)
                : VariableSizeObjectStore(1.0f, filename, openFlags) {
            // Ignore fill degree. We always pack with 100%
        }

        ~PaCHashObjectStore() override {
            delete index;
        }

        static std::string name() {
            return "PaCHashObjectStore a=" + std::to_string(a) + " indexStructure=" + Index::name();
        }

        size_t key2bin(StoreConfig::key_t key) {
            #ifdef __SIZEOF_INT128__ // fastrange64
                static_assert(sizeof(key) == sizeof(uint64_t));
                return (uint64_t)(((__uint128_t)key * (__uint128_t)numBins) >> 64);
            #else
                return key / (~size_t(0)/numBins);
            #endif
        }

        template <class Iterator, typename HashFunction, typename LengthExtractor, typename ValuePointerExtractor,
                class U = typename std::iterator_traits<Iterator>::value_type>
        void writeToFile(Iterator begin, Iterator end, HashFunction hashFunction,
                         LengthExtractor lengthExtractor, ValuePointerExtractor valuePointerExtractor) {
            static_assert(std::is_invocable_r_v<StoreConfig::key_t, HashFunction, U>);
            static_assert(std::is_invocable_r_v<size_t, LengthExtractor, U>);
            static_assert(std::is_invocable_r_v<const char *, ValuePointerExtractor, U>);

            constructionTimer.notifyStartConstruction();
            constructionTimer.notifyDeterminedSpace();
            numObjects = end - begin;
            LOG("Sorting input keys");
            ips2ra::sort(begin, end, hashFunction);

            constructionTimer.notifyPlacedObjects();

            LOG("Writing");
            LinearObjectWriter writer(filename, openFlags);
            Iterator it = begin;
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = hashFunction(*it);
                assert(key != 0); // Key 0 holds metadata
                size_t length = lengthExtractor(*it);
                totalPayloadSize += length;
                const char *content = valuePointerExtractor(*it);
                writer.write(key, length, content);
                LOG("Writing", i, numObjects);
                it++;
            }
            writer.close(StoreMetadata::TYPE_PACHASH);
            constructionTimer.notifyWroteObjects();
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
            StoreMetadata metadata = readMetadata(filename);
            if (metadata.type != StoreMetadata::TYPE_PACHASH) {
                throw std::logic_error("Opened file of wrong type");
            }
            numBlocks = metadata.numBlocks;
            maxSize = metadata.maxSize;
            numBins = numBlocks * a;

            #ifdef HAS_LIBURING
            UringDoubleBufferBlockIterator blockIterator(filename, numBlocks, 2500, openFlags);
            #else
            PosixBlockIterator blockIterator(filename, numBlocks, openFlags);
            #endif
            index = new Index(numBlocks, numBins);
            size_t keysRead = 0;
            StoreConfig::key_t lastKeyInPreviousBlock = 0;
            for (size_t blocksRead = 0; blocksRead < numBlocks; blocksRead++) {
                BlockStorage block(blockIterator.blockContent());

                size_t lastBinInPreviousBlock = key2bin(lastKeyInPreviousBlock);
                if (block.numObjects > 0 && block.offsets[0] == 0) {
                    size_t firstBinInThisBlock = key2bin(block.keys[0]);
                    if (firstBinInThisBlock - lastBinInPreviousBlock >= 1) {
                        // Empty bin between both blocks. Optimization: Account the empty bin to the current block,
                        // so that when reading the first full bin of the current block or the last full bin
                        // of the previous block, we do not need to load the other block unnecessarily.
                        index->push_back(firstBinInThisBlock - 1);
                    } else {
                        index->push_back(lastBinInPreviousBlock);
                    }
                } else {
                    index->push_back(lastBinInPreviousBlock);
                }
                if (block.numObjects > 0) {
                    StoreConfig::key_t key = block.keys[block.numObjects - 1];
                    // Last block can contain 0 again as a terminator for the last object
                    assert(key > lastKeyInPreviousBlock || blocksRead == numBlocks - 1);
                    lastKeyInPreviousBlock = key;
                }
                keysRead += block.numObjects;
                if (blocksRead < numBlocks - 1) {
                    blockIterator.next(); // Don't try to read more than the last one
                }
                LOG("Reading", blocksRead, numBlocks);
            }
            LOG(nullptr);
            numObjects = keysRead;

            index->complete();
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return (double)index->space() * 8.0 / numBlocks;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout << "RAM space usage: " << bytehamster::util::prettyBytes(index->space())
                    << " (" << internalSpaceUsage() << " bits/block)" << std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return 4 * (maxSize + StoreConfig::BLOCK_LENGTH - 1);
        }

        size_t requiredIosPerQuery() override {
            return 1;
        }

        template <typename IoManager>
        void enqueueQuery(QueryHandle *handle, IoManager ioManager) {
            assert(handle->state == 0 && "Used handle that did not go through awaitCompletion()");
            handle->state = 1;
            handle->stats.notifyStartQuery();
            std::tuple<size_t, size_t> accessDetails;
            index->locate(key2bin(handle->key), accessDetails);

            size_t blocksAccessed = std::get<1>(accessDetails);
            size_t blockStartPosition = std::get<0>(accessDetails) * StoreConfig::BLOCK_LENGTH;
            size_t searchRangeLength = blocksAccessed * StoreConfig::BLOCK_LENGTH;
            assert(blocksAccessed <= requiredBufferPerQuery() * StoreConfig::BLOCK_LENGTH);
            handle->stats.notifyFoundBlock(blocksAccessed);

            // Using the resultPointers as a temporary store.
            handle->length = blocksAccessed;
            ioManager->enqueueRead(handle->buffer, blockStartPosition, searchRangeLength,
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

    private:
        inline void reconstruct(QueryHandle *&handle, size_t &i, BlockStorage &block,
                                size_t &blockIdx, char *&blockPtr, size_t &blocksAccessed) {
            if (i < size_t(block.numObjects - 1)) {
                // Object does not overlap. We already have the size
                // and the pointer does not need reconstruction. All is nice and easy.
                handle->length = block.offsets[i + 1] - block.offsets[i];
                handle->resultPtr = blockPtr + block.offsets[i];
                assert(handle->length <= maxSize);
                handle->stats.notifyFoundKey();
                handle->state = 0;
                return;
            } else {
                // Object overlaps. We need to find the size by examining the following blocks.
                // Also, we need to reconstruct the object to remove the headers in the middle.
                char *resultPtr = blockPtr + block.offsets[i];
                size_t length = block.tableStart - resultPtr - block.emptyPageEnd;

                blockIdx++;
                for(; blockIdx < blocksAccessed; blockIdx++) {
                    char *nextBlockPtr = handle->buffer + blockIdx * StoreConfig::BLOCK_LENGTH;
                    BlockStorage nextBlock(nextBlockPtr);
                    if (nextBlock.numObjects > 0) {
                        // We found the next object and therefore the end of this one.
                        StoreConfig::offset_t lengthOnNextBlock = nextBlock.offsets[0];
                        memmove(resultPtr + length, nextBlock.blockStart, lengthOnNextBlock);
                        length += lengthOnNextBlock;

                        handle->length = length;
                        handle->resultPtr = resultPtr;
                        assert(length <= maxSize);
                        handle->stats.notifyFoundKey();
                        handle->state = 0;
                        return;
                    } else {
                        // Fully overlapped. We have to copy the whole block and continue searching.
                        size_t lengthOnNextBlock = nextBlock.tableStart - nextBlock.blockStart;
                        memmove(resultPtr + length, nextBlock.blockStart, lengthOnNextBlock);
                        length += lengthOnNextBlock - nextBlock.emptyPageEnd;
                    }
                }

                // Special case. Object fills the last loaded block exactly. We did not load the next one.
                handle->length = length;
                handle->resultPtr = resultPtr;
                assert(handle->length <= maxSize);
                handle->stats.notifyFoundKey();
                handle->state = 0;
                return;
            }
        }

        inline void parse(QueryHandle *handle) {
            handle->stats.notifyFetchedBlock();
            size_t blocksAccessed = handle->length;

            for (size_t blockIdx = 0; blockIdx < blocksAccessed; blockIdx++) {
                char *blockPtr = handle->buffer + blockIdx * StoreConfig::BLOCK_LENGTH;
                BlockStorage block(blockPtr);
                for (size_t i = 0; i < block.numObjects; i++) {
                    if (handle->key == block.keys[i]) [[unlikely]] {
                        reconstruct(handle, i, block, blockIdx, blockPtr, blocksAccessed);
                        return;
                    }
                }
            }

            // Did not find object
            handle->length = 0;
            handle->resultPtr = nullptr;
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};

} // Namespace pachash
