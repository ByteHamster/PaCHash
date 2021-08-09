#ifndef TESTCOMPARISON_ELIASFANOINDEXING_H
#define TESTCOMPARISON_ELIASFANOINDEXING_H

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

#define TEST_IDENTIFIER size_t(numObjects + averageSize + (distribution << 16))

template <uint16_t a, class IoManager = MemoryMapIO<>>
class EliasFanoIndexing : public VariableSizeObjectStore {
    public:
        static constexpr size_t MAX_PAGES_ACCESSED = 4;
        EliasFano<ceillog2(a)> firstBinInBucketEf;
        std::unique_ptr<IoManager> ioManager = nullptr;
        char *objectReconstructionBuffer;
        char *pageReadBuffer;
        size_t totalPayloadSize = 0;
        size_t numBuckets = 0;
        size_t numBins = 0;

        QueryTimer queryTimer;
        size_t bytesSearched = 0;
        size_t maxBytesSearched = 0;
        size_t bucketsAccessed = 0;
        size_t bucketsAccessedUnnecessary = 0;
        size_t elementsOverlappingBucketBoundaries = 0;

        EliasFanoIndexing(size_t numObjects, size_t averageSize)
                : VariableSizeObjectStore(numObjects, averageSize) {
            objectReconstructionBuffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, PageConfig::MAX_SIMULTANEOUS_QUERIES * PageConfig::MAX_OBJECT_SIZE * sizeof(char)));
            pageReadBuffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, PageConfig::MAX_SIMULTANEOUS_QUERIES * MAX_PAGES_ACCESSED * PageConfig::PAGE_SIZE * sizeof(char)));
            std::cout<<"Constructing EliasFanoIndexing<"<<IoManager::NAME()<<"> with a="<<(int)a<<", N="<<(double)numObjects<<", L="<<averageSize<<std::endl;
        }

        ~EliasFanoIndexing() {
            free(objectReconstructionBuffer);
            free(pageReadBuffer);
        }

        uint64_t key2bin(uint64_t key) {
            #ifdef __SIZEOF_INT128__ // fastrange64
                return (uint64_t)(((__uint128_t)key * (__uint128_t)numBins) >> 64);
            #else
                return key / (UINT64_MAX/numBins);
            #endif
        }

        void generateInputData() final {
            int fd = open(INPUT_FILE, O_RDONLY);
            if (fd >= 0) {
                struct stat fileStat = {};
                fstat(fd, &fileStat);
                if (fileStat.st_size > sizeof(PageConfig::offset_t)) {
                    char *file = static_cast<char *>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
                    size_t *identifierPtr = reinterpret_cast<size_t *>(&file[sizeof(PageConfig::offset_t)]);
                    size_t identifierRead = *identifierPtr;
                    munmap(file, fileStat.st_size);
                    if (identifierRead == TEST_IDENTIFIER) {
                        close(fd);
                        return;
                    }
                }
            }
            close(fd);

            if (SHOW_PROGRESS) std::cout<<"# Generating input keys"<<std::flush;
            std::mt19937_64 generator(std::random_device{}());
            std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
            std::vector<uint64_t> keys;
            keys.reserve(numObjects);
            for (size_t i = 0; i < numObjects; i++) {
                keys.push_back(dist(generator));
            }

            if (SHOW_PROGRESS) std::cout<<"\r# Sorting input keys"<<std::flush;
            ips2ra::sort(keys.begin(), keys.end());

            PagedFileOutputStream file(INPUT_FILE, numObjects * averageSize);
            size_t identifier = 42; // Temporary identifier while file is not fully written yet
            file.write(identifier);

            std::vector<size_t> lengthDistribution(50 * averageSize);
            size_t maxLength = 0;
            size_t minLength = ~0;
            for (size_t i = 0; i < numObjects; i++) {
                file.notifyObjectStart();
                uint64_t key = keys.at(i);
                std::string value = Distribution::valueFor<distribution>(key, averageSize);
                uint16_t length = value.length();
                file.write(ObjectHeader{key, length});
                file.write(value.data(), value.size());

                if (((i % (numObjects/PROGRESS_STEPS)) == 0 || i == numObjects - 1) && SHOW_PROGRESS) {
                    std::cout<<"\r# Writing: "<<key<<" ("<<round(100.0*i/numObjects)<<"%)"<<std::flush;
                }

                maxLength = std::max(maxLength, (size_t) length);
                minLength = std::min(minLength, (size_t) length);
                lengthDistribution.at(length)++;
            }
            identifier = TEST_IDENTIFIER;
            file.writeAt(sizeof(PageConfig::offset_t), identifier);
            file.close();

            /*for (size_t i = minLength; i <= maxLength; i++) {
                std::cout<<i<<": "<<lengthDistribution.at(i)<<std::endl;
            }*/
        }

        void reloadInputDataFromFile() final {
            int fd = open(INPUT_FILE, O_RDONLY);
            struct stat fileStat = {};
            fstat(fd, &fileStat);
            char *file = static_cast<char *>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
            madvise(file, fileStat.st_size, MADV_SEQUENTIAL);
            PagedObjectReconstructor pageReader(file, objectReconstructionBuffer);

            numBuckets = fileStat.st_size / PageConfig::PAGE_SIZE + 1;
            numBins = numBuckets * a;

            firstBinInBucketEf.reserve(numBuckets, numBins);
            keysTestingOnly.reserve(numObjects);

            size_t lastBucketWritten = -1;
            size_t keysRead = 0;
            while (pageReader.position < fileStat.st_size && keysRead < numObjects) {
                size_t objectStartPosition = pageReader.position;
                ObjectHeader *header = pageReader.read<ObjectHeader>();
                keysTestingOnly.push_back(header->key);
                totalPayloadSize += header->length;
                assert(header->length < 5000);

                #ifndef NDEBUG
                    if (keysRead < 1000 || (rand() % 1000) == 0) {
                        // This is so slow, only perform the check for some of the objects.
                        ObjectHeader headerCopy = *header;
                        // When both key and value overlap bucket boundaries, this overwrites the header pointer.
                        // We therefore need a copy of the header here.
                        std::string value(pageReader.read(headerCopy.length), headerCopy.length);
                        assert(value == Distribution::valueFor<distribution>(headerCopy.key, averageSize));
                        header = &headerCopy;
                    } else {
                        pageReader.skip(header->length);
                    }
                #else
                    pageReader.skip(header->length);
                #endif

                size_t objectEndPosition = pageReader.position;

                if ((objectEndPosition / PageConfig::PAGE_SIZE) != (objectStartPosition / PageConfig::PAGE_SIZE)) {
                    elementsOverlappingBucketBoundaries++;
                }

                size_t endingBucket = objectEndPosition / PageConfig::PAGE_SIZE;
                size_t bin = key2bin(header->key);
                if (lastBucketWritten != endingBucket) {
                    assert(lastBucketWritten + 1 == endingBucket); // TODO: Support huge objects
                    firstBinInBucketEf.push_back(bin);
                    lastBucketWritten = endingBucket;
                }
                keysRead++;
                if (((keysRead % (numObjects / PROGRESS_STEPS)) == 1 || keysRead == numObjects - 1) && SHOW_PROGRESS) {
                    std::cout << "\r# Reading: " << std::round(100.0 * keysRead / numObjects) << "%" << std::flush;
                }
            }
            std::cout << "\r";
            if (keysRead != numObjects) {
                std::cerr<<"Error: Not enough pre-generated keys"<<std::endl;
            }

            munmap(file, fileStat.st_size);
            close(fd);
            // Generate select data structure
            firstBinInBucketEf.predecessorPosition(key2bin(keysTestingOnly.at(0)));
            ioManager = std::make_unique<IoManager>(INPUT_FILE);
        }

        void printConstructionStats() final {
            std::cout<<"External space usage: "<<prettyBytes(numBuckets * PageConfig::PAGE_SIZE)<<std::endl;
            std::cout<<"Objects overlapping bucket boundaries: "<<(double)elementsOverlappingBucketBoundaries*100/numObjects<<"%"<<std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/numObjects<<std::endl;
            std::cout<<"RAM space usage: "
                     <<prettyBytes(firstBinInBucketEf.space())<<" ("
                     <<(double)firstBinInBucketEf.space()*8/numBuckets<<" bits/block)"<<std::endl;
        }

        inline std::tuple<size_t, size_t> findBlocksToAccess(uint64_t key) {
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
            return std::make_tuple(iPtr, accessed);
        }

        std::tuple<size_t, char *> findKeyWithinBlock(uint64_t key, std::tuple<size_t, size_t> accessDetails,
                                                      char *block, char *reconstructionBuffer) {
            size_t blocksAccessed = std::get<1>(accessDetails);
            size_t blockStartPosition = std::get<0>(accessDetails)*PageConfig::PAGE_SIZE;
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

            size_t absolutePosition = pageReader.position + blockStartPosition;
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

        std::vector<std::tuple<size_t, char *>> query(std::vector<uint64_t> &keys) final {
            assert(keys.size() <= PageConfig::MAX_SIMULTANEOUS_QUERIES);
            std::vector<std::tuple<size_t, char *>> result(keys.size());
            queryTimer.notifyStartQuery(keys.size());
            std::tuple<size_t, size_t> accessDetails[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                accessDetails[i] = findBlocksToAccess(keys.at(i));
            }
            queryTimer.notifyFoundBlock();
            char *blockContents[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                size_t blocksAccessed = std::get<1>(accessDetails[i]);
                size_t blockStartPosition = std::get<0>(accessDetails[i])*PageConfig::PAGE_SIZE;
                size_t searchRangeLength = blocksAccessed*PageConfig::PAGE_SIZE;
                assert(blocksAccessed <= MAX_PAGES_ACCESSED);
                bucketsAccessed += blocksAccessed;
                blockContents[i] = ioManager->readBlocks(blockStartPosition, searchRangeLength,
                                   pageReadBuffer + i * MAX_PAGES_ACCESSED * PageConfig::PAGE_SIZE);
            }
            ioManager->awaitCompletionOfReadRequests();
            queryTimer.notifyFetchedBlock();
            for (int i = 0; i < keys.size(); i++) {
                result.at(i) = findKeyWithinBlock(keys.at(i), accessDetails[i], blockContents[i],
                  objectReconstructionBuffer + i * PageConfig::MAX_OBJECT_SIZE);
            }
            queryTimer.notifyFoundKey();
            return result;
        }

        void printQueryStats() final {
            std::cout<<"Average bytes searched per query: "<<(double)bytesSearched/queryTimer.numQueries<<" ("<<maxBytesSearched<<" max)"<<std::endl;
            std::cout<<"Average buckets accessed per query: "<<(double)bucketsAccessed/queryTimer.numQueries<<std::endl;
            std::cout<<"Queries with unnecessary bucket access: "<<(double)bucketsAccessedUnnecessary*100/queryTimer.numQueries<<"%"<<std::endl;
            queryTimer.print();
        }
};

#ifdef TEST_BUCKETS
static void bucketProbability() {
    int queries = 2e6;
    double avgSize = 50;

    std::default_random_engine generator;
    std::normal_distribution<double> normalDist(avgSize,1.0);

    std::cout<<"n;experiment;estimate;estimateRound"<<std::endl;
    for (double expectedNumElements = 1.0; expectedNumElements <= 6; expectedNumElements += 0.2) {
        int resultsAfterMidpoint = 0;
        std::normal_distribution<double> normalDistNumElements(expectedNumElements,1.0);

        double fractExpectedNumElements = expectedNumElements - std::floor(expectedNumElements);
        double bucketsWithRoundedDown = 1-fractExpectedNumElements;
        double bucketsWithRoundedUp = fractExpectedNumElements;
        double elementsInBucketsWithRoundedDown = bucketsWithRoundedDown * std::floor(expectedNumElements);
        double elementsInBucketsWithRoundedUp = bucketsWithRoundedUp * std::ceil(expectedNumElements);
        double probabilityOfRoundedUpBucket = elementsInBucketsWithRoundedUp/(elementsInBucketsWithRoundedDown+elementsInBucketsWithRoundedUp);

        for (int q = 0; q < queries; q++) {
            int numElements = std::floor(expectedNumElements) + ((((double)rand()/(double)RAND_MAX) < probabilityOfRoundedUpBucket) ? 1 : 0);

            int sizePrefix[numElements + 1];
            sizePrefix[0] = 0;
            for (int i = 0; i < numElements; i++) {
                sizePrefix[i + 1] = sizePrefix[i] + std::abs(std::round(normalDist(generator)));
            }

            int sizeSum = sizePrefix[numElements];
            int cutpoint = (rand() % sizeSum) + 1;
            int queriedElement = rand() % numElements;
            if (sizePrefix[queriedElement] >= cutpoint) {
                resultsAfterMidpoint++;
            }
        }
        std::cout << std::fixed << std::setprecision(1) << std::setfill(' ') << std::setw(4);
        std::cout<<expectedNumElements;

        double experiment = (double)resultsAfterMidpoint/queries;
        double estimate = (0.5*(expectedNumElements - 1))/expectedNumElements;

        double estimateRoundDown = (0.5*(floor(expectedNumElements) - 1))/floor(expectedNumElements);
        double estimateRoundUp = (0.5*(ceil(expectedNumElements) - 1))/ceil(expectedNumElements);
        double estimateRound = probabilityOfRoundedUpBucket * estimateRoundUp + (1-probabilityOfRoundedUpBucket) * estimateRoundDown;

        std::cout << std::fixed << std::setprecision(4) << std::setfill(' ') << std::setw(7);
        std::cout<<";"<<experiment<<";"<<estimate<<";"<<estimateRound<<std::endl;
    }
}
#endif

#endif //TESTCOMPARISON_ELIASFANOINDEXING_H
