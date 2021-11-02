#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <cstdio>
#include <fcntl.h>

#include "GccDiagnostics.h"
DIAGNOSTICS_DISABLE
#include <ips2ra.hpp>
DIAGNOSTICS_ENABLE

#include "EliasFano.h"
#include "IoManager.h"
#include "Util.h"
#include "VariableSizeObjectStore.h"
#include "LinearObjectWriter.h"
#include "BlockIterator.h"

/**
 * Store the first bin intersecting with each block with Elias-Fano.
 * Execute a predecessor query to retrieve the key location.
 */
template <uint16_t a>
class EliasFanoObjectStore : public VariableSizeObjectStore {
    public:
        using Super = VariableSizeObjectStore;
        static constexpr size_t MAX_BLOCKS_ACCESSED = 4 * (StoreConfig::MAX_OBJECT_SIZE + StoreConfig::BLOCK_LENGTH - 1) / StoreConfig::BLOCK_LENGTH;
        static constexpr size_t FANO_SIZE = ceillog2(a);
        EliasFano<FANO_SIZE> *firstBinInBlockEf = nullptr;
        size_t numBins = 0;

        explicit EliasFanoObjectStore([[maybe_unused]] float fillDegree, const char* filename, int openFlags)
                : VariableSizeObjectStore(1.0f, filename, openFlags) {
            // Ignore fill degree. We always pack with 100%
        }

        static std::string name() {
            return "EliasFanoObjectStore a=" + std::to_string(a);
        }

        size_t key2bin(StoreConfig::key_t key) {
            #ifdef __SIZEOF_INT128__ // fastrange64
                static_assert(sizeof(key) == sizeof(uint64_t));
                return (uint64_t)(((__uint128_t)key * (__uint128_t)numBins) >> 64);
            #else
                return key / (~size_t(0)/numBins);
            #endif
        }

        void writeToFile(std::vector<StoreConfig::key_t> &keys, ObjectProvider &objectProvider) final {
            constructionTimer.notifyStartConstruction();
            constructionTimer.notifyDeterminedSpace();
            numObjects = keys.size();
            LOG("Sorting input keys");
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#pragma GCC diagnostic ignored "-Wpedantic"-Wimplicit-fallthrough
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
            ips2ra::sort(keys.begin(), keys.end());
#pragma GCC diagnostic pop

            constructionTimer.notifyPlacedObjects();

            LOG("Writing");
            LinearObjectWriter writer(filename, openFlags);
            for (size_t i = 0; i < numObjects; i++) {
                StoreConfig::key_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                StoreConfig::length_t length = objectProvider.getLength(key);
                totalPayloadSize += length;
                const char *content = objectProvider.getValue(key);
                writer.write(key, length, content);
                LOG("Writing", i, numObjects);
            }
            writer.close();
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            numBlocks = readSpecialObject0(filename);
            numBins = numBlocks * a;

            UringDoubleBufferBlockIterator blockIterator(filename, numBlocks, 2500, openFlags);
            firstBinInBlockEf = new EliasFano<ceillog2(a)>(numBlocks, numBins);
            firstBinInBlockEf->add(0, key2bin(0));
            size_t keysRead = 0;
            StoreConfig::key_t previousRead = 0;
            for (size_t blocksRead = 0; blocksRead < numBlocks; blocksRead++) {
                BlockStorage block(blockIterator.blockContent());
                if (blockIterator.blockNumber() == numBlocks - 1) {
                    // Ignore last one
                } else if (block.numObjects > 0) {
                    size_t bin = key2bin(block.keys[block.numObjects - 1]);
                    assert(bin < numBins);
                    firstBinInBlockEf->add(blockIterator.blockNumber() + 1, bin);
                    previousRead = bin;
                } else {
                    firstBinInBlockEf->add(blockIterator.blockNumber() + 1, previousRead);
                }
                keysRead += block.numObjects;
                if (blocksRead < numBlocks - 1) {
                    blockIterator.next(); // Don't try to read more than the last one
                }
                LOG("Reading", blocksRead, numBlocks);
            }
            LOG(nullptr);
            numObjects = keysRead;

            // Generate select data structure
            firstBinInBlockEf->predecessorPosition(key2bin(0));
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return (double)firstBinInBlockEf->space() * 8.0 / numBlocks;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout << "RAM space usage: " << prettyBytes(firstBinInBlockEf->space()) << " (" << internalSpaceUsage() << " bits/block)" << std::endl;
            std::cout << "Therefrom select data structure: " << prettyBytes(firstBinInBlockEf->selectStructureOverhead())
                      << " (" << 100.0 * firstBinInBlockEf->selectStructureOverhead() / firstBinInBlockEf->space() << "%)" << std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return MAX_BLOCKS_ACCESSED * StoreConfig::BLOCK_LENGTH;
        }

        size_t requiredIosPerQuery() override {
            return 1;
        }

    private:
        inline void findBlocksToAccess(std::tuple<size_t, size_t> *output, StoreConfig::key_t key) {
            const size_t bin = key2bin(key);
            auto iPtr = firstBinInBlockEf->predecessorPosition(bin);
            auto jPtr = iPtr;
            if (iPtr > 0 && *iPtr == bin) {
                --iPtr;
            }
            while (jPtr < numBlocks - 1) {
                auto nextPointer = jPtr;
                ++nextPointer;
                if (*nextPointer > bin) {
                    break;
                }
                jPtr = nextPointer;
            }
            size_t accessed = jPtr - iPtr + 1;
            std::get<0>(*output) = iPtr;
            std::get<1>(*output) = accessed;
        }

    public:
        template <typename IoManager>
        void submitSingleQuery(QueryHandle *handle, IoManager ioManager) {
            if (handle->state != 0) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            handle->state = 1;
            handle->stats.notifyStartQuery();
            std::tuple<size_t, size_t> accessDetails;
            findBlocksToAccess(&accessDetails, handle->key);

            size_t blocksAccessed = std::get<1>(accessDetails);
            size_t blockStartPosition = std::get<0>(accessDetails) * StoreConfig::BLOCK_LENGTH;
            size_t searchRangeLength = blocksAccessed * StoreConfig::BLOCK_LENGTH;
            assert(blocksAccessed <= MAX_BLOCKS_ACCESSED);
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

        void parse(QueryHandle *handle) {
            handle->stats.notifyFetchedBlock();

            size_t blocksAccessed = handle->length;
            size_t currentBlock = 0;
            std::tuple<StoreConfig::length_t, char *> result;
            do {
                result = findKeyWithinBlock(handle->key, handle->buffer + currentBlock * StoreConfig::BLOCK_LENGTH);
                currentBlock++;
            } while (std::get<1>(result) == nullptr && currentBlock < blocksAccessed);

            StoreConfig::length_t length = std::get<0>(result);
            char *resultPtr = std::get<1>(result);
            handle->length = length;
            handle->resultPtr = resultPtr;

            assert(length <= StoreConfig::MAX_OBJECT_SIZE);
            if (resultPtr == nullptr) {
                handle->stats.notifyFoundKey();
                handle->state = 0;
                return;
            }
            size_t offsetInBuffer = resultPtr - handle->buffer;
            size_t offsetInBlock = offsetInBuffer % StoreConfig::BLOCK_LENGTH;
            char *block = resultPtr - offsetInBlock;
            BlockStorage blockStorage(block);
            StoreConfig::length_t reconstructed = std::min(static_cast<StoreConfig::length_t>(blockStorage.tableStart - resultPtr), length);
            char *nextBlockStart = block + StoreConfig::BLOCK_LENGTH;
            while (reconstructed < length) {
                // Element overlaps bucket boundaries.
                // The read buffer is just used for this object, so we can concatenate the object destructively.
                BlockStorage nextBlock(nextBlockStart);
                StoreConfig::length_t spaceInNextBlock = (nextBlock.tableStart - nextBlock.blockStart);
                assert(spaceInNextBlock <= StoreConfig::BLOCK_LENGTH);
                StoreConfig::length_t spaceToCopy = std::min(static_cast<StoreConfig::length_t>(length - reconstructed), spaceInNextBlock);
                assert(spaceToCopy > 0 && spaceToCopy <= StoreConfig::MAX_OBJECT_SIZE);
                memmove(resultPtr + reconstructed, nextBlock.blockStart, spaceToCopy);
                reconstructed += spaceToCopy;
                nextBlockStart += StoreConfig::BLOCK_LENGTH;
                assert(reconstructed <= StoreConfig::MAX_OBJECT_SIZE);
            }
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};
