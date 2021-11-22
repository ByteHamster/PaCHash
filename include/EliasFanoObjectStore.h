#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <cstdio>
#include <fcntl.h>
#include <ips2ra.hpp>
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
        static constexpr size_t FANO_SIZE = ceillog2(a);
        EliasFano<FANO_SIZE> *firstBinInBlockEf = nullptr;
        size_t numBins = 0;

        explicit EliasFanoObjectStore([[maybe_unused]] float fillDegree, const char* filename, int openFlags)
                : VariableSizeObjectStore(1.0f, filename, openFlags) {
            // Ignore fill degree. We always pack with 100%
        }

        ~EliasFanoObjectStore() {
            delete firstBinInBlockEf;
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

        template <class Iterator, typename HashFunction, typename LengthExtractor, typename ValuePointerExtractor,
                class U = typename std::iterator_traits<Iterator>::value_type>
        void writeToFile(Iterator begin, Iterator end, HashFunction hashFunction,
                         LengthExtractor lengthExtractor, ValuePointerExtractor valuePointerExtractor) {
            static_assert(std::is_same<U, std::decay_t<std::tuple_element_t<0, typename function_traits<HashFunction>::arg_tuple>>>::value, "Hash function must get argument of type U");
            static_assert(std::is_same<StoreConfig::key_t, std::decay_t<typename function_traits<HashFunction>::result_type>>::value, "Hash function must return StoreConfig::key_t");

            static_assert(std::is_same<U, std::decay_t<std::tuple_element_t<0, typename function_traits<LengthExtractor>::arg_tuple>>>::value, "Length extractor must get argument of type U");
            static_assert(std::is_same<StoreConfig::length_t, std::decay_t<typename function_traits<LengthExtractor>::result_type>>::value, "Length extractor must return StoreConfig::length_t");

            static_assert(std::is_same<U, std::decay_t<std::tuple_element_t<0, typename function_traits<ValuePointerExtractor>::arg_tuple>>>::value, "Value extractor must get argument of type U");
            static_assert(std::is_same<const char *, std::decay_t<typename function_traits<ValuePointerExtractor>::result_type>>::value, "Value extractor must return const char *");

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
                StoreConfig::length_t length = lengthExtractor(*it);
                totalPayloadSize += length;
                const char *content = valuePointerExtractor(*it);
                writer.write(key, length, content);
                LOG("Writing", i, numObjects);
                it++;
            }
            writer.close();
            constructionTimer.notifyWroteObjects();
        }

        void writeToFile(std::vector<std::pair<std::string, std::string>> &vector) {
            auto hashFunction = [](const std::pair<std::string, std::string> &x) -> StoreConfig::key_t {
                return MurmurHash64(std::get<0>(x).data(), std::get<0>(x).length());
            };
            auto lengthEx = [](const std::pair<std::string, std::string> &x) -> StoreConfig::length_t {
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
            numBlocks = metadata.numBlocks;
            maxSize = metadata.maxSize;
            numBins = numBlocks * a;

            #ifdef HAS_LIBURING
            UringDoubleBufferBlockIterator blockIterator(filename, numBlocks, 2500, openFlags);
            #else
            PosixBlockIterator blockIterator(filename, numBlocks, openFlags);
            #endif
            firstBinInBlockEf = new EliasFano<ceillog2(a)>(numBlocks, numBins);
            size_t keysRead = 0;
            StoreConfig::key_t lastKeyInPreviousBlock = 0;
            for (size_t blocksRead = 0; blocksRead < numBlocks; blocksRead++) {
                BlockStorage block(blockIterator.blockContent());

                size_t lastBinInPreviousBlock = key2bin(lastKeyInPreviousBlock);
                if (block.offset == 0 && block.numObjects > 0) {
                    size_t firstBinInThisBlock = key2bin(block.keys[0]);
                    if (firstBinInThisBlock - lastBinInPreviousBlock >= 1) {
                        // Empty bin between both blocks. Optimization: Account the empty bin to the current block,
                        // so that when reading the first full bin of the current block or the last full bin
                        // of the previous block, we do not need to load the other block unnecessarily.
                        firstBinInBlockEf->push_back(firstBinInThisBlock - 1);
                    } else {
                        firstBinInBlockEf->push_back(lastBinInPreviousBlock);
                    }
                } else {
                    firstBinInBlockEf->push_back(lastBinInPreviousBlock);
                }
                if (block.numObjects > 0) {
                    StoreConfig::key_t key = block.keys[block.numObjects - 1];
                    assert(key > lastKeyInPreviousBlock);
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
            return 4 * (maxSize + StoreConfig::BLOCK_LENGTH - 1);
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
        void enqueueQuery(QueryHandle *handle, IoManager ioManager) {
            assert(handle->state == 0 && "Used handle that did not go through awaitCompletion()");
            handle->state = 1;
            handle->stats.notifyStartQuery();
            std::tuple<size_t, size_t> accessDetails;
            findBlocksToAccess(&accessDetails, handle->key);

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

        inline void parse(QueryHandle *handle) {
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

            assert(length <= maxSize);
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
                assert(spaceToCopy > 0 && spaceToCopy <= maxSize);
                memmove(resultPtr + reconstructed, nextBlock.blockStart, spaceToCopy);
                reconstructed += spaceToCopy;
                nextBlockStart += StoreConfig::BLOCK_LENGTH;
                assert(reconstructed <= maxSize);
            }
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};
