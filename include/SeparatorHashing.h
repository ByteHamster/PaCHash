#ifndef TESTCOMPARISON_SEPARATORHASHING_H
#define TESTCOMPARISON_SEPARATORHASHING_H

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "Hash.h"
#include "PageConfig.h"
#include "VariableSizeObjectStore.h"
#include "FixedBlockObjectStore.h"
#include "IoManager.h"

#define INCREMENTAL_INSERT

template <size_t separatorBits = 6, class IoManager = MemoryMapIO<>>
class SeparatorHashing : public FixedBlockObjectStore {
    private:
        QueryTimer queryTimer;
        size_t totalPayloadSize = 0;
        size_t numInternalProbes = 0;
        char *pageReadBuffer;
        std::vector<FixedBlockObjectStore::Item> insertionQueue;
        sdsl::int_vector<separatorBits> separators;
        std::unique_ptr<IoManager> ioManager = nullptr;
    public:
        explicit SeparatorHashing(size_t numObjects, size_t averageSize, float fillDegree)
                : FixedBlockObjectStore(numObjects, averageSize, fillDegree) {
            pageReadBuffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, PageConfig::MAX_SIMULTANEOUS_QUERIES * PageConfig::PAGE_SIZE * sizeof(char)));
            std::cout<<"Constructing SeparatorHashing<"<<IoManager::NAME()<<"> with sepBits="<<separatorBits<<", alpha="<<fillDegree<<", N="<<(double)numObjects<<", L="<<averageSize<<std::endl;
        }

        ~SeparatorHashing() {
            free(pageReadBuffer);
        }

        void generateInputData(std::vector<std::pair<uint64_t, size_t>> &keysAndLengths,
                               std::function<const char*(uint64_t)> valuePointer) final {
            buckets.resize(numBuckets);
            std::default_random_engine generator(std::random_device{}());
            std::uniform_int_distribution<uint64_t> uniformDist(0, UINT64_MAX);

            separators = sdsl::int_vector<separatorBits>(numBuckets, (1 << separatorBits) - 1);
            for (int i = 0; i < numObjects; i++) {
                uint64_t key = keysAndLengths.at(i).first;
                size_t size = keysAndLengths.at(i).second;
                totalPayloadSize += size;

                #ifdef INCREMENTAL_INSERT
                    insert(key, size);
                #else
                    size_t bucket = Hash::hash(key, 0, numBuckets);
                    buckets.at(bucket).items.push_back(Item{key, size, 0});
                    buckets.at(bucket).length += size + sizeof(ObjectHeader);
                #endif

                if (SHOW_PROGRESS && (i % (numObjects/PROGRESS_STEPS) == 0 || i == numObjects - 1)) {
                    std::cout<<"\r# Inserting "<<std::round(100.0*i/numObjects)<<"%"<<std::flush;
                }
            }

            #ifndef INCREMENTAL_INSERT
                std::cout<<"\r# Repairing"<<std::flush;
                for (int i = 0; i < numBuckets; i++) {
                    if (buckets.at(i).length > BUCKET_SIZE) {
                        handleOverflowingBucket(i);
                    }
                }
                std::cout<<"\r# Handling insertion queue"<<std::flush;
                handleInsertionQueue();
            #endif

            writeBuckets(valuePointer);
        }

        void reloadInputDataFromFile() final {
            int fd = open(INPUT_FILE, O_RDONLY);
            struct stat fileStat = {};
            fstat(fd, &fileStat);
            char *file = static_cast<char *>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
            madvise(file, fileStat.st_size, MADV_SEQUENTIAL);

            size_t objectsFound = 0;
            size_t position = 0;
            separators = sdsl::int_vector<separatorBits>(numBuckets, 0);
            while (position + sizeof(ObjectHeader) < fileStat.st_size) {
                ObjectHeader *header = reinterpret_cast<ObjectHeader *>(&file[position]);

                size_t sizeLeftInBucket = PageConfig::PAGE_SIZE - position % PageConfig::PAGE_SIZE;
                assert(sizeLeftInBucket <= PageConfig::PAGE_SIZE);
                if ((header->length == 0 && header->key == 0) || sizeLeftInBucket < sizeof(ObjectHeader)) {
                    // Skip rest of page
                    position += sizeLeftInBucket;
                    assert(position % PageConfig::PAGE_SIZE == 0);
                } else {
                    assert(header->length < PageConfig::PAGE_SIZE);
                    objectsFound++;
                    size_t bucket = position / PageConfig::PAGE_SIZE;
                    separators[bucket] = std::max(uint64_t(separators[bucket]), separator(header->key, bucket) + 1);
                    position += header->length + sizeof(ObjectHeader);
                }
                if (SHOW_PROGRESS && (objectsFound % (numObjects/PROGRESS_STEPS) == 0 || objectsFound == numObjects - 1)) {
                    std::cout<<"\r# Reading "<<std::round(100.0*objectsFound/numObjects)<<"%"<<std::flush;
                }
            }
            std::cout << "\r";
            assert(objectsFound == numObjects);
            munmap(file, fileStat.st_size);
            close(fd);
            ioManager = std::make_unique<IoManager>(INPUT_FILE);
        }

        void printConstructionStats() final {
            std::cout<<"External space usage: "<<prettyBytes(numBuckets*PageConfig::PAGE_SIZE)<<" ("
                     <<(double)100*(totalPayloadSize + numObjects*sizeof(ObjectHeader))/(numBuckets*PageConfig::PAGE_SIZE)<<"% utilization)"<<std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/numObjects<<std::endl;
            std::cout<<"RAM space usage: "
                     <<prettyBytes(separators.capacity()/8)<<" ("<<separatorBits<<" bits/block, scaled: "
                     <<(double)separatorBits/fillDegree<<" bits/block)"<<std::endl;
        }

        void insert(uint64_t key, size_t length) {
            insert({key, length, 0});
        }

        uint64_t separator(uint64_t key, size_t bucket) {
            return Hash::hash(key, bucket, (1 << separatorBits) - 1);
        }

        void insert(FixedBlockObjectStore::Item item) {
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                FixedBlockObjectStore::Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t bucket = Hash::hash(item.key, item.currentHashFunction, numBuckets);
                while (separator(item.key, bucket) >= separators[bucket]) {
                    // We already bumped items from this bucket. We therefore cannot insert new ones with larger separator
                    item.currentHashFunction++;
                    bucket = Hash::hash(item.key, item.currentHashFunction, numBuckets);

                    if (item.currentHashFunction > 100) {
                        // Empirically, making this number larger does not increase the success probability
                        // but increases the duration of failed construction attempts significantly.
                        std::cout<<std::endl;
                        std::cerr<<"Unable to insert item. Please reduce the load factor and try again."<<std::endl;
                        exit(1);
                    }
                }

                buckets.at(bucket).items.push_back(item);
                buckets.at(bucket).length += item.length + sizeof(ObjectHeader);

                if (buckets.at(bucket).length > PageConfig::PAGE_SIZE) {
                    handleOverflowingBucket(bucket);
                }
            }
        }

        void handleOverflowingBucket(size_t bucket) {
            if (buckets.at(bucket).length <= PageConfig::PAGE_SIZE) {
                return;
            }
            std::sort(buckets.at(bucket).items.begin(), buckets.at(bucket).items.end(),
                      [&]( const auto& lhs, const auto& rhs ) {
                          return separator(lhs.key, bucket) < separator(rhs.key, bucket);
                      });

            size_t sizeSum = 0;
            size_t i = 0;
            size_t tooLargeItemSeparator = -1;
            for (;i < buckets.at(bucket).items.size(); i++) {
                sizeSum += buckets.at(bucket).items.at(i).length + sizeof(ObjectHeader);
                if (sizeSum > PageConfig::PAGE_SIZE) {
                    tooLargeItemSeparator = separator(buckets.at(bucket).items.at(i).key, bucket);
                    break;
                }
            }
            assert(tooLargeItemSeparator != -1);

            sizeSum = 0;
            i = 0;
            for (;i < buckets.at(bucket).items.size(); i++) {
                if (separator(buckets.at(bucket).items.at(i).key, bucket) >= tooLargeItemSeparator) {
                    break;
                }
                sizeSum += buckets.at(bucket).items.at(i).length + sizeof(ObjectHeader);
            }

            std::vector<FixedBlockObjectStore::Item> overflow(buckets.at(bucket).items.begin() + i, buckets.at(bucket).items.end());
            assert(buckets.at(bucket).items.size() == overflow.size() + i);

            buckets.at(bucket).items.resize(i);
            buckets.at(bucket).length = sizeSum;
            separators[bucket] = tooLargeItemSeparator;
            for (FixedBlockObjectStore::Item overflowedItem : overflow) {
                overflowedItem.currentHashFunction++;
                insertionQueue.push_back(overflowedItem);
            }
        }

        size_t findBlockToAccess(uint64_t key) {
            for (size_t hashFunctionIndex = 0; hashFunctionIndex < 100000; hashFunctionIndex++) {
                size_t bucket = Hash::hash(key, hashFunctionIndex, numBuckets);
                numInternalProbes++;
                if (separator(key, bucket) < separators[bucket]) {
                    return bucket;
                }
            }
            std::cerr<<"Unable to find block"<<std::endl;
            return -1;
        }

        std::vector<std::tuple<size_t, char *>> query(std::vector<uint64_t> &keys) final {
            assert(keys.size() <= PageConfig::MAX_SIMULTANEOUS_QUERIES);
            size_t bucketIndexes[keys.size()];
            queryTimer.notifyStartQuery(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                bucketIndexes[i] = findBlockToAccess(keys.at(i));
            }
            queryTimer.notifyFoundBlock();
            char *blockContents[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                blockContents[i] = ioManager->readBlocks(bucketIndexes[i] * PageConfig::PAGE_SIZE,
                         PageConfig::PAGE_SIZE, pageReadBuffer + i * PageConfig::PAGE_SIZE);
            }
            ioManager->awaitCompletionOfReadRequests();
            queryTimer.notifyFetchedBlock();
            std::vector<std::tuple<size_t, char *>> result(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                result.at(i) = findKeyWithinBlock(keys.at(i), blockContents[i]);
            }
            queryTimer.notifyFoundKey();
            return result;
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<1
                <<" ("<<(double)numInternalProbes/queryTimer.numQueries<<" internal probes)"<<std::endl;
            queryTimer.print();
        }
};

#endif // TESTCOMPARISON_SEPARATORHASHING_H