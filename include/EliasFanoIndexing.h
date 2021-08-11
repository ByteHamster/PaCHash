#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <cstdio>
#include <fcntl.h>
#include <ips2ra.hpp>

#include "EliasFano.h"
#include "PagedFileOutputStream.h"
#include "PagedObjectReconstructor.h"
#include "IoManager.h"
#include "Util.h"
#include "VariableSizeObjectStore.h"

/**
 * Store the first bin intersecting with each bucket with Elias-Fano.
 * Execute a predecessor query to retrieve the key location.
 */
template <uint16_t a, class Config = VariableSizeObjectStoreConfig>
class EliasFanoIndexing : public VariableSizeObjectStore<Config> {
    public:
        using Super = VariableSizeObjectStore<Config>;
        static constexpr size_t MAX_PAGES_ACCESSED = 4;
        EliasFano<ceillog2(a)> firstBinInBucketEf;
        size_t totalPayloadSize = 0;
        size_t numBuckets = 0;
        size_t numBins = 0;
        std::vector<char *> objectReconstructionBuffers;
        QueryTimer queryTimer;
        size_t bytesSearched = 0;
        size_t maxBytesSearched = 0;
        size_t bucketsAccessed = 0;
        size_t bucketsAccessedUnnecessary = 0;
        size_t elementsOverlappingBucketBoundaries = 0;

        explicit EliasFanoIndexing(const char* filename) : VariableSizeObjectStore<Config>(filename) {
        }

        ~EliasFanoIndexing() {
            for (int i = 0; i < this->numQueryHandles; i++) {
                free(objectReconstructionBuffers.at(i));
            }
        }

        uint64_t key2bin(uint64_t key) {
            #ifdef __SIZEOF_INT128__ // fastrange64
                return (uint64_t)(((__uint128_t)key * (__uint128_t)numBins) >> 64);
            #else
                return key / (UINT64_MAX/numBins);
            #endif
        }

        void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
            if constexpr (Config::SHOW_PROGRESS) {
                std::cout<<"Constructing EliasFanoIndexing<"<<Config::IoManager::NAME()<<"> with a="<<(int)a<<std::endl;
            }
            this->numObjects = keys.size();
            this->LOG("Sorting input keys");
            ips2ra::sort(keys.begin(), keys.end());

            // Knowing the average object length is just a performance improvement for construction
            // and not actually necessary for the method to work.
            size_t averageObjectLength = 0;
            size_t numSamples = 10;
            for (size_t i = 0; i < numSamples; i++) {
                averageObjectLength += objectProvider.getLength(keys.at(random() % keys.size()));
            }
            averageObjectLength /= numSamples;

            PagedFileOutputStream file(this->filename, this->numObjects * averageObjectLength);
            for (size_t i = 0; i < this->numObjects; i++) {
                file.notifyObjectStart();
                uint64_t key = keys.at(i);
                uint64_t length = objectProvider.getLength(key);
                file.write(ObjectHeader{key, static_cast<uint16_t>(length)});
                file.write(objectProvider.getValue(key), length);
                this->LOG("Writing", i, this->numObjects);
            }
            this->LOG("Flushing and closing file");
            file.close();
        }

        void reloadFromFile() final {
            int fd = open(this->filename, O_RDONLY);
            struct stat fileStat = {};
            fstat(fd, &fileStat);
            char *file = static_cast<char *>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
            madvise(file, fileStat.st_size, MADV_SEQUENTIAL);
            char *objectReconstructionBuffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, PageConfig::MAX_OBJECT_SIZE * sizeof(char)));
            PagedObjectReconstructor pageReader(file, objectReconstructionBuffer);

            numBuckets = fileStat.st_size / PageConfig::PAGE_SIZE + 1;
            numBins = numBuckets * a;

            firstBinInBucketEf.reserve(numBuckets, numBins);
            size_t lastBucketWritten = -1;
            size_t keysRead = 0;
            uint64_t lastKey = 0;
            while (pageReader.position < fileStat.st_size && keysRead < this->numObjects) {
                size_t objectStartPosition = pageReader.position;
                ObjectHeader *header = pageReader.read<ObjectHeader>();
                totalPayloadSize += header->length;
                assert(header->length < 5000);
                pageReader.skip(header->length);

                size_t objectEndPosition = pageReader.position;

                if ((objectEndPosition / PageConfig::PAGE_SIZE) != (objectStartPosition / PageConfig::PAGE_SIZE)) {
                    elementsOverlappingBucketBoundaries++;
                }

                size_t endingBucket = objectEndPosition / PageConfig::PAGE_SIZE;
                lastKey = header->key;
                size_t bin = key2bin(header->key);
                if (lastBucketWritten != endingBucket) {
                    assert(lastBucketWritten + 1 == endingBucket); // TODO: Support huge objects
                    firstBinInBucketEf.push_back(bin);
                    lastBucketWritten = endingBucket;
                }
                keysRead++;
                this->LOG("Reading", keysRead, this->numObjects);
            }
            this->LOG(nullptr);
            if (keysRead != this->numObjects) {
                std::cerr<<"Error: Not enough pre-generated keys"<<std::endl;
            }

            munmap(file, fileStat.st_size);
            close(fd);
            free(objectReconstructionBuffer);
            // Generate select data structure
            firstBinInBucketEf.predecessorPosition(key2bin(lastKey));
        }

        void printConstructionStats() final {
            std::cout<<"External space usage: "<<prettyBytes(numBuckets * PageConfig::PAGE_SIZE)<<std::endl;
            std::cout<<"Objects overlapping bucket boundaries: "<<(double)elementsOverlappingBucketBoundaries*100/this->numObjects<<"%"<<std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/this->numObjects<<std::endl;
            std::cout<<"RAM space usage: "
                     <<prettyBytes(firstBinInBucketEf.space())<<" ("
                     <<(double)firstBinInBucketEf.space()*8/numBuckets<<" bits/block)"<<std::endl;
        }

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

        std::tuple<size_t, char *> findKeyWithinBlock(uint64_t key, size_t blocksAccessed, char *block, char *reconstructionBuffer) {
            size_t searchRangeLength = blocksAccessed*PageConfig::PAGE_SIZE;
            PagedObjectReconstructor pageReader(block, reconstructionBuffer);

            char *resultObjectPointer;
            ObjectHeader *currentHeader;
            ObjectHeader finalHeader = {0, 0};
            while (pageReader.position < searchRangeLength) {
                currentHeader = pageReader.read<ObjectHeader>();
                if (key == currentHeader->key) {
                    finalHeader = *currentHeader;
                    resultObjectPointer = pageReader.read(finalHeader.length);
                    break; // Could return here, the rest of the function only does statistics
                } else {
                    pageReader.skip(currentHeader->length);
                }
            }
            if (pageReader.position >= searchRangeLength) {
                resultObjectPointer = nullptr;
            }

            size_t absolutePosition = pageReader.position + 10*PageConfig::PAGE_SIZE; // Just that it does not underflow
            size_t accessNeeded = absolutePosition/PageConfig::PAGE_SIZE
                                  - (absolutePosition - finalHeader.length + sizeof(ObjectHeader))/PageConfig::PAGE_SIZE + 1;
            assert(blocksAccessed >= accessNeeded);
            size_t unnecessaryAccess = blocksAccessed - accessNeeded;
            bucketsAccessedUnnecessary += unnecessaryAccess;
            size_t searched = pageReader.position;
            bytesSearched += searched;
            maxBytesSearched = std::max(maxBytesSearched, searched);
            if (pageReader.position >= searchRangeLength) {
                // Read more than we actually fetched
                std::cerr<<"Error: Read too much: "<<key<<std::endl;
            }
            return std::make_tuple(finalHeader.length, resultObjectPointer);
        }

        QueryHandle newQueryHandle(size_t batchSize) final {
            objectReconstructionBuffers.push_back((char *)aligned_alloc(PageConfig::PAGE_SIZE, PageConfig::MAX_OBJECT_SIZE * sizeof(char)));
            return Super::newQueryHandle(batchSize);
        }

        void submitQuery(QueryHandle &handle) final {
            if (!handle.completed) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            handle.completed = false;
            queryTimer.notifyStartQuery(handle.keys.size());
            std::tuple<size_t, size_t> accessDetails[handle.keys.size()];
            for (int i = 0; i < handle.keys.size(); i++) {
                findBlocksToAccess(&accessDetails[i], handle.keys.at(i));
            }
            queryTimer.notifyFoundBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                size_t blocksAccessed = std::get<1>(accessDetails[i]);
                size_t blockStartPosition = std::get<0>(accessDetails[i])*PageConfig::PAGE_SIZE;
                size_t searchRangeLength = blocksAccessed*PageConfig::PAGE_SIZE;
                assert(blocksAccessed <= MAX_PAGES_ACCESSED);
                bucketsAccessed += blocksAccessed;
                // Using the resultPointers as a temporary store.
                handle.resultLengths.at(i) = blocksAccessed;
                handle.resultPointers.at(i) = this->ioManagers.at(handle.handleId)->enqueueRead(blockStartPosition,
                    searchRangeLength, this->pageReadBuffers.at(handle.handleId) + i * MAX_PAGES_ACCESSED * PageConfig::PAGE_SIZE);
            }
            this->ioManagers.at(handle.handleId)->submit();
        }

        void awaitCompletion(QueryHandle &handle) final {
            if (handle.completed) {
                return;
            }
            this->ioManagers.at(handle.handleId)->awaitCompletion();
            queryTimer.notifyFetchedBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                size_t blocksAccessed = handle.resultLengths.at(i);
                char *blockContents = handle.resultPointers.at(i);
                std::tuple<size_t, char *> result = findKeyWithinBlock(handle.keys.at(i), blocksAccessed, blockContents,
                   objectReconstructionBuffers.at(handle.handleId) + i * PageConfig::MAX_OBJECT_SIZE);
                handle.resultLengths.at(i) = std::get<0>(result);
                handle.resultPointers.at(i) = std::get<1>(result);
            }
            queryTimer.notifyFoundKey();
            handle.completed = true;
        }

        void printQueryStats() final {
            std::cout<<"Average bytes searched per query: "<<(double)bytesSearched/queryTimer.numQueries<<" ("<<maxBytesSearched<<" max)"<<std::endl;
            std::cout<<"Average buckets accessed per query: "<<(double)bucketsAccessed/queryTimer.numQueries<<std::endl;
            std::cout<<"Queries with unnecessary bucket access: "<<(double)bucketsAccessedUnnecessary*100/queryTimer.numQueries<<"%"<<std::endl;
            queryTimer.print();
        }
};
