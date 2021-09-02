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
            return "EliasFanoObjectStore (a=" + std::to_string(a) + ")";
        }

        uint64_t key2bin(uint64_t key) {
            #ifdef __SIZEOF_INT128__ // fastrange64
                return (uint64_t)(((__uint128_t)key * (__uint128_t)numBins) >> 64);
            #else
                return key / (UINT64_MAX/numBins);
            #endif
        }

        void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
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
            writeBuckets(objectProvider, true);
        }

        void reloadFromFile() final {
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
        }

        float internalSpaceUsage() final {
            return (double)firstBinInBucketEf.space()*8.0/numBuckets;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"Objects overlapping bucket boundaries: "<<(double)elementsOverlappingBucketBoundaries*100/this->numObjects<<"%"<<std::endl;
            std::cout<<"RAM space usage: "<<prettyBytes(firstBinInBucketEf.space())<<" ("<<internalSpaceUsage()<<" bits/block)"<<std::endl;
        }

        /**
         * Create a new handle that can execute \p batchSize queries simultaneously.
         * Multiple handles can be used to execute multiple batches simultaneously.
         * It is advisable to batch queries instead of executing them one-by-one using a QueryHandle each.
         * Query handle creation is an expensive operation and should be done before the actual queries.
         */
        template <typename IoManager = MemoryMapIO>
        QueryHandle *newQueryHandle(size_t batchSize, int openFlags = 0) {
            QueryHandle *handle = new QueryHandle(*this, batchSize);
            handle->ioManager = std::make_unique<IoManager>(openFlags, batchSize, MAX_PAGES_ACCESSED * PageConfig::PAGE_SIZE, this->filename);
            queryHandles.push_back(handle);
            return handle;
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

    protected:
        void submitQuery(QueryHandle &handle) final {
            if (!handle.completed) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            handle.completed = false;
            numQueries += handle.keys.size();
            handle.stats.notifyStartQuery(handle.keys.size());
            std::tuple<size_t, size_t> accessDetails[handle.keys.size()];
            for (int i = 0; i < handle.keys.size(); i++) {
                findBlocksToAccess(&accessDetails[i], handle.keys.at(i));
            }
            handle.stats.notifyFoundBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                size_t blocksAccessed = std::get<1>(accessDetails[i]);
                size_t blockStartPosition = std::get<0>(accessDetails[i])*PageConfig::PAGE_SIZE;
                size_t searchRangeLength = blocksAccessed*PageConfig::PAGE_SIZE;
                assert(blocksAccessed <= MAX_PAGES_ACCESSED);
                bucketsAccessed += blocksAccessed;
                // Using the resultPointers as a temporary store.
                handle.resultLengths.at(i) = blocksAccessed;
                handle.resultPointers.at(i) = handle.ioManager->enqueueRead(
                        blockStartPosition, searchRangeLength);
            }
            handle.ioManager->submit();
        }

        void awaitCompletion(QueryHandle &handle) final {
            if (handle.completed) {
                return;
            }
            handle.ioManager->awaitCompletion();
            handle.stats.notifyFetchedBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                size_t blocksAccessed = handle.resultLengths.at(i);
                char *blockContents = handle.resultPointers.at(i);
                size_t currentBlock = 0;
                std::tuple<size_t, char *> result;
                do {
                    result = findKeyWithinBlock(handle.keys.at(i), blockContents + currentBlock * PageConfig::PAGE_SIZE);
                    currentBlock++;
                } while (std::get<1>(result) == nullptr && currentBlock < blocksAccessed);
                size_t length = std::get<0>(result);
                char *pointer = std::get<1>(result);
                size_t pointerInt = reinterpret_cast<size_t>(pointer);
                size_t startBucket = pointerInt/PageConfig::PAGE_SIZE;
                size_t endBucket = (pointerInt+length)/PageConfig::PAGE_SIZE;
                if (startBucket != endBucket) {
                    // Element overlaps bucket boundaries.
                    // The read buffer is just used for this object, so we can concatenate the object destructively.
                    // TODO: This is not true for mmap
                    assert(startBucket + 1 == endBucket); // TODO: Larger objects
                    size_t spaceLeftInBucket = PageConfig::PAGE_SIZE - (pointerInt % PageConfig::PAGE_SIZE);
                    char *nextBucketStart = pointer+spaceLeftInBucket;
                    uint16_t numObjectsOnNextPage = *reinterpret_cast<uint16_t *>(nextBucketStart + sizeof(uint16_t));
                    size_t nextPageHeaderSize = overheadPerPage + numObjectsOnNextPage*overheadPerObject;
                    memmove(pointer + spaceLeftInBucket, nextBucketStart + nextPageHeaderSize, length - spaceLeftInBucket);
                }
                handle.resultLengths.at(i) = length;
                handle.resultPointers.at(i) = pointer;
            }
            handle.stats.notifyFoundKey();
            handle.completed = true;
        }
};
