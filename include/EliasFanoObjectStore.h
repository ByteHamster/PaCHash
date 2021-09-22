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

/**
 * Store the first bin intersecting with each bucket with Elias-Fano.
 * Execute a predecessor query to retrieve the key location.
 */
template <uint16_t a>
class EliasFanoObjectStore : public VariableSizeObjectStore {
    public:
        using Super = VariableSizeObjectStore;
        static constexpr size_t MAX_PAGES_ACCESSED = 4;
        EliasFano<ceillog2(a)> firstBinInBucketEf;
        size_t numBins = 0;
        size_t numQueries = 0;
        size_t bucketsAccessed = 0;
        size_t elementsOverlappingBucketBoundaries = 0;

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

            this->LOG("Calculating buckets");
            size_t bucketSize = 0;
            size_t bucket = 0;
            numBuckets = 1;
            totalPayloadSize = 0;
            buckets.push_back(Bucket{});
            for (size_t i = 0; i < numObjects; i++) {
                uint64_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                size_t length = objectProvider.getLength(key);
                totalPayloadSize += length;
                bucketSize += length + overheadPerObject;
                buckets.at(bucket).items.push_back(Item{key, length});
                buckets.at(bucket).length += length + overheadPerObject;

                size_t maxBucketSize = PageConfig::PAGE_SIZE - overheadPerPage;
                if (bucket == 0) {
                    maxBucketSize -= overheadPerObject + sizeof(MetadataObjectType);
                }
                if (bucketSize + overheadPerObject >= maxBucketSize) { // No more objects fit into this bucket
                    bucket++;
                    buckets.push_back(Bucket{});
                    numBuckets++;
                    if (bucketSize > maxBucketSize) {
                        size_t overflow = bucketSize - maxBucketSize;
                        bucketSize = overflow;
                    } else {
                        bucketSize = 0;
                    }
                }
            }
            constructionTimer.notifyPlacedObjects();
            writeBuckets(objectProvider, true);
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
            int fd = open(this->filename, O_RDONLY);
            numBuckets = readSpecialObject0(filename);
            numBins = numBuckets * a;
            size_t fileSize = numBuckets * PageConfig::PAGE_SIZE;

            char *file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
            madvise(file, fileSize, MADV_SEQUENTIAL);

            firstBinInBucketEf.reserve(numBuckets, numBins);
            firstBinInBucketEf.push_back(key2bin(0));
            size_t keysRead = 0;
            for (size_t bucket = 0; bucket < numBuckets - 1; bucket++) {
                char *bucketStart = file + PageConfig::PAGE_SIZE * bucket;
                uint16_t objectsInBucket = *reinterpret_cast<uint16_t *>(&bucketStart[0 + sizeof(uint16_t)]);
                uint64_t lastKey = *reinterpret_cast<size_t *>(&bucketStart[overheadPerPage + objectsInBucket*sizeof(uint16_t) + (objectsInBucket-1)*sizeof(uint64_t)]);
                // Assume that last key always overlaps into the next bucket (approximation)
                firstBinInBucketEf.push_back(key2bin(lastKey));
                keysRead += objectsInBucket;
                this->LOG("Reading", bucket, numBuckets);
            }
            this->LOG(nullptr);
            this->numObjects = keysRead;
            munmap(file, fileSize);
            close(fd);
            // Generate select data structure
            firstBinInBucketEf.predecessorPosition(key2bin(0));
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return (double)firstBinInBucketEf.space()*8.0/numBuckets;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"Objects overlapping bucket boundaries: "<<(double)elementsOverlappingBucketBoundaries*100/this->numObjects<<"%"<<std::endl;
            std::cout<<"RAM space usage: "<<prettyBytes(firstBinInBucketEf.space())<<" ("<<internalSpaceUsage()<<" bits/block)"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return MAX_PAGES_ACCESSED * PageConfig::PAGE_SIZE;
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<(double)bucketsAccessed/numQueries<<std::endl;
        }

    private:
        inline void findBlocksToAccess(std::tuple<size_t, size_t> *output, uint64_t key) {
            const size_t bin = key2bin(key);
            auto iPtr = firstBinInBucketEf.predecessorPosition(bin);
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
        void submitSingleQuery(QueryHandle *handle) final {
            if (ioManager == nullptr) {
                ioManager = new PosixIO(filename, 0, 10);
            }
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

        QueryHandle *peekAny() final {
            QueryHandle *handle = reinterpret_cast<QueryHandle *>(ioManager->peekAny());
            if (handle != nullptr) {
                parse(handle);
            }
            return handle;
        }

        QueryHandle *awaitAny() final {
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
            handle->length = std::get<0>(result);
            handle->resultPtr = std::get<1>(result);
            size_t offsetInBuffer = reinterpret_cast<size_t>(handle->resultPtr) - reinterpret_cast<size_t>(handle->buffer);
            if ((offsetInBuffer % PageConfig::PAGE_SIZE) + handle->length > PageConfig::PAGE_SIZE) {
                // Element overlaps bucket boundaries.
                // The read buffer is just used for this object, so we can concatenate the object destructively.
                assert(handle->length < PageConfig::PAGE_SIZE); // TODO: Larger objects that overlap more than one boundary
                size_t spaceLeftInBucket = PageConfig::PAGE_SIZE - (offsetInBuffer % PageConfig::PAGE_SIZE);
                char *nextBucketStart = handle->buffer + PageConfig::PAGE_SIZE;
                uint16_t numObjectsOnNextPage = *reinterpret_cast<uint16_t *>(nextBucketStart + sizeof(uint16_t));
                size_t nextPageHeaderSize = overheadPerPage + numObjectsOnNextPage*overheadPerObject;
                assert(nextPageHeaderSize < PageConfig::PAGE_SIZE);
                size_t spaceToCopy = handle->length - spaceLeftInBucket;
                assert(spaceToCopy <= PageConfig::MAX_OBJECT_SIZE);
                memmove(handle->resultPtr + spaceLeftInBucket, nextBucketStart + nextPageHeaderSize, spaceToCopy);
            }
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};
