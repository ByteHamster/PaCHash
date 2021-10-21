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
 * Store the first bin intersecting with each bucket with Elias-Fano.
 * Execute a predecessor query to retrieve the key location.
 */
template <uint16_t a>
class EliasFanoObjectStore : public VariableSizeObjectStore {
    public:
        using Super = VariableSizeObjectStore;
        static constexpr size_t MAX_PAGES_ACCESSED = 4 * (PageConfig::MAX_OBJECT_SIZE + PageConfig::PAGE_SIZE - 1)/PageConfig::PAGE_SIZE;
        static constexpr size_t FANO_SIZE = ceillog2(a);
        EliasFano<FANO_SIZE> *firstBinInBucketEf = nullptr;
        size_t numBins = 0;
        size_t numQueries = 0;
        size_t bucketsAccessed = 0;

        explicit EliasFanoObjectStore([[maybe_unused]] float fillDegree, const char* filename)
                : VariableSizeObjectStore(1.0f, filename) {
            // Ignore fill degree. We always pack with 100%
        }

        static std::string name() {
            return "EliasFanoObjectStore a=" + std::to_string(a);
        }

        uint64_t key2bin(uint64_t key) {
            #ifdef __SIZEOF_INT128__ // fastrange64
                return (uint64_t)(((__uint128_t)key * (__uint128_t)numBins) >> 64);
            #else
                return key / (UINT64_MAX/numBins);
            #endif
        }

        void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
            constructionTimer.notifyStartConstruction();
            constructionTimer.notifyDeterminedSpace();
            this->numObjects = keys.size();
            this->LOG("Sorting input keys");
            ips2ra::sort(keys.begin(), keys.end());
            constructionTimer.notifyPlacedObjects();

            this->LOG("Writing");
            LinearObjectWriter writer(filename);
            for (size_t i = 0; i < numObjects; i++) {
                uint64_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                size_t length = objectProvider.getLength(key);
                totalPayloadSize += length;
                const char *content = objectProvider.getValue(key);
                writer.write(key, length, content);
                this->LOG("Writing", i, numObjects);
            }
            writer.close();
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            numBuckets = readSpecialObject0(filename);
            numBins = numBuckets * a;

            UringDoubleBufferBlockIterator blockIterator(filename, numBuckets, 2500);
            firstBinInBucketEf = new EliasFano<ceillog2(a)>(numBuckets, numBins);
            firstBinInBucketEf->add(0, key2bin(0));
            size_t keysRead = 0;
            uint64_t previousRead = 0;
            for (size_t bucketsRead = 0; bucketsRead < numBuckets; bucketsRead++) {
                BlockStorage bucket(blockIterator.bucketContent());
                if (blockIterator.bucketNumber() == numBuckets - 1) {
                    // Ignore last one
                } else if (bucket.numObjects > 0) {
                    size_t bin = key2bin(bucket.keys[bucket.numObjects - 1]);
                    assert(bin < numBins);
                    firstBinInBucketEf->add(blockIterator.bucketNumber() + 1, bin);
                    previousRead = bin;
                } else {
                    firstBinInBucketEf->add(blockIterator.bucketNumber() + 1, previousRead);
                }
                keysRead += bucket.numObjects;
                if (bucketsRead < numBuckets - 1) {
                    blockIterator.next(); // Don't try to read more than the last one
                }
                this->LOG("Reading", bucketsRead, numBuckets);
            }
            this->LOG(nullptr);
            this->numObjects = keysRead;

            // Generate select data structure
            firstBinInBucketEf->predecessorPosition(key2bin(0));
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return (double)firstBinInBucketEf->space()*8.0/numBuckets;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: "<<prettyBytes(firstBinInBucketEf->space())<<" ("<<internalSpaceUsage()<<" bits/block)"<<std::endl;
            std::cout<<"Therefrom select data structure: "<<prettyBytes(firstBinInBucketEf->selectStructureOverhead())
                        <<" ("<<100.0*firstBinInBucketEf->selectStructureOverhead()/firstBinInBucketEf->space()<<"%)"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return MAX_PAGES_ACCESSED * PageConfig::PAGE_SIZE;
        }

        size_t requiredIosPerQuery() override {
            return 1;
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<(double)bucketsAccessed/numQueries<<std::endl;
        }

    private:
        inline void findBlocksToAccess(std::tuple<size_t, size_t> *output, uint64_t key) {
            const size_t bin = key2bin(key);
            auto iPtr = firstBinInBucketEf->predecessorPosition(bin);
            auto jPtr = iPtr;
            if (iPtr > 0 && *iPtr == bin) {
                --iPtr;
            }
            while (jPtr < numBuckets - 1) {
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
            numQueries++;
            handle->stats.notifyStartQuery();
            std::tuple<size_t, size_t> accessDetails;
            findBlocksToAccess(&accessDetails, handle->key);
            handle->stats.notifyFoundBlock();

            size_t blocksAccessed = std::get<1>(accessDetails);
            size_t blockStartPosition = std::get<0>(accessDetails)*PageConfig::PAGE_SIZE;
            size_t searchRangeLength = blocksAccessed*PageConfig::PAGE_SIZE;
            assert(blocksAccessed <= MAX_PAGES_ACCESSED);
            bucketsAccessed += blocksAccessed;

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
            std::tuple<size_t, char *> result;
            do {
                result = findKeyWithinBlock(handle->key, handle->buffer + currentBlock * PageConfig::PAGE_SIZE);
                currentBlock++;
            } while (std::get<1>(result) == nullptr && currentBlock < blocksAccessed);

            size_t length = std::get<0>(result);
            char *resultPtr = std::get<1>(result);
            handle->length = length;
            handle->resultPtr = resultPtr;

            assert(length <= PageConfig::MAX_OBJECT_SIZE);
            if (resultPtr == nullptr) {
                handle->stats.notifyFoundKey();
                handle->state = 0;
                return;
            }
            size_t offsetInBuffer = resultPtr - handle->buffer;
            size_t offsetOnPage = offsetInBuffer % PageConfig::PAGE_SIZE;
            char *page = resultPtr - offsetOnPage;
            BlockStorage pageStorage(page);
            size_t reconstructed = std::min(static_cast<size_t>(pageStorage.tableStart - resultPtr), length);
            char *nextBucketStart = page + PageConfig::PAGE_SIZE;
            while (reconstructed < length) {
                // Element overlaps bucket boundaries.
                // The read buffer is just used for this object, so we can concatenate the object destructively.
                BlockStorage nextBlock(nextBucketStart);
                size_t spaceInNextBucket = (nextBlock.tableStart - nextBlock.pageStart);
                assert(spaceInNextBucket <= PageConfig::PAGE_SIZE);
                size_t spaceToCopy = std::min(length - reconstructed, spaceInNextBucket);
                assert(spaceToCopy > 0 && spaceToCopy <= PageConfig::MAX_OBJECT_SIZE);
                memmove(resultPtr + reconstructed, nextBlock.pageStart, spaceToCopy);
                reconstructed += spaceToCopy;
                nextBucketStart += PageConfig::PAGE_SIZE;
                assert(reconstructed <= PageConfig::MAX_OBJECT_SIZE);
            }
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};
