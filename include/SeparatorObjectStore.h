#pragma once

#include <vector>
#include <random>
#include <sdsl/bit_vectors.hpp>

#include "PageConfig.h"
#include "VariableSizeObjectStore.h"
#include "IoManager.h"

#define INCREMENTAL_INSERT

/**
 * For each bucket, store a separator hash that determines whether an element is stored in the bucket or must
 * continue looking through its probe sequence.
 * See: "File organization: Implementation of a method guaranteeing retrieval in one access" (Larson, Kajla)
 */
template <size_t separatorBits = 6>
class SeparatorObjectStore : public VariableSizeObjectStore {
    private:
        using Super = VariableSizeObjectStore;
        using Item = typename Super::Item;
        size_t numQueries = 0;
        size_t numInternalProbes = 0;
        std::vector<Item> insertionQueue;
        sdsl::int_vector<separatorBits> separators;
    public:
        using QueryHandle = typename Super::QueryHandle;

        explicit SeparatorObjectStore(float fillDegree, const char* filename)
                : VariableSizeObjectStore(fillDegree, filename) {
        }

        static std::string name() {
            return "SeparatorObjectStore (s=" + std::to_string(separatorBits) + ")";
        }

        void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
            LOG("Calculating total size to determine number of blocks");
            numObjects = keys.size();
            size_t spaceNeeded = 0;
            for (unsigned long key : keys) {
                spaceNeeded += objectProvider.getLength(key);
            }
            spaceNeeded += keys.size() * overheadPerObject;
            spaceNeeded += spaceNeeded/PageConfig::PAGE_SIZE*overheadPerPage;
            this->numBuckets = (spaceNeeded / this->fillDegree) / PageConfig::PAGE_SIZE;
            this->buckets.resize(this->numBuckets);

            std::uniform_int_distribution<uint64_t> uniformDist(0, UINT64_MAX);
            separators = sdsl::int_vector<separatorBits>(this->numBuckets, (1 << separatorBits) - 1);
            for (int i = 0; i < this->numObjects; i++) {
                uint64_t key = keys.at(i);
                assert(key != 0); // Key 0 holds metadata
                size_t size = objectProvider.getLength(key);
                totalPayloadSize += size;

                #ifdef INCREMENTAL_INSERT
                    insert(key, size);
                #else
                    size_t bucket = fastrange64(MurmurHash64Seeded(key, 0), numBuckets);
                    buckets.at(bucket).items.push_back(Item{key, size, 0});
                    buckets.at(bucket).length += size + overheadPerObject;
                #endif

                LOG("Inserting", i, this->numObjects);
            }

            #ifndef INCREMENTAL_INSERT
                LOG("Repairing");
                for (int i = 0; i < this->numBuckets; i++) {
                    if (this->buckets.at(i).length > BUCKET_SIZE) {
                        handleOverflowingBucket(i);
                    }
                }
                LOG("Handling insertion queue");
                handleInsertionQueue();
            #endif

            this->writeBuckets(objectProvider, false);
        }

        void reloadFromFile() final {
            numBuckets = readSpecialObject0(filename);
            size_t fileSize = numBuckets * PageConfig::PAGE_SIZE;

            int fd = open(this->filename, O_RDONLY);
            char *file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
            madvise(file, fileSize, MADV_SEQUENTIAL);

            size_t objectsFound = 0;
            separators = sdsl::int_vector<separatorBits>(this->numBuckets, 0);
            for (size_t bucket = 0; bucket < numBuckets; bucket++) {
                char *bucketStart = file + PageConfig::PAGE_SIZE * bucket;
                uint16_t objectsInBucket = *reinterpret_cast<uint16_t *>(&bucketStart[0 + sizeof(uint16_t)]);
                int maxSeparator = -1;
                for (size_t i = 0; i < objectsInBucket; i++) {
                    uint64_t key = *reinterpret_cast<size_t *>(&bucketStart[overheadPerPage + objectsInBucket*sizeof(uint16_t) + i*sizeof(uint64_t)]);
                    if (key != 0) { // Key 0 holds metadata
                        maxSeparator = std::max(maxSeparator, (int) separator(key, bucket));
                        objectsFound++;
                    }
                }
                separators[bucket] = maxSeparator + 1;
                this->LOG("Reading", bucket, numBuckets);
            }
            this->LOG(nullptr);
            this->numObjects = objectsFound;
            munmap(file, fileSize);
            close(fd);
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: "
                     <<prettyBytes(separators.capacity()/8)<<" ("<<separatorBits<<" bits/block, scaled: "
                     <<(double)separatorBits/this->fillDegree<<" bits/block)"<<std::endl;
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
            handle->ioManager = std::make_unique<IoManager>(openFlags, batchSize, PageConfig::PAGE_SIZE, this->filename);
            queryHandles.push_back(handle);
            return handle;
        }

        void printQueryStats() final {
            std::cout<<"Average buckets accessed per query: "<<1
                <<" ("<<(double)numInternalProbes/numQueries<<" internal probes)"<<std::endl;
        }

    private:
        void insert(uint64_t key, size_t length) {
            insert({key, length, 0});
        }

        uint64_t separator(uint64_t key, size_t bucket) {
            return fastrange64(MurmurHash64Seeded(key, bucket), (1 << separatorBits) - 1);
        }

        void insert(Item item) {
            insertionQueue.push_back(item);
            handleInsertionQueue();
        }

        void handleInsertionQueue() {
            while (!insertionQueue.empty()) {
                Item item = insertionQueue.back();
                insertionQueue.pop_back();

                size_t bucket = fastrange64(MurmurHash64Seeded(item.key, item.currentHashFunction), this->numBuckets);
                while (separator(item.key, bucket) >= separators[bucket]) {
                    // We already bumped items from this bucket. We therefore cannot insert new ones with larger separator
                    item.currentHashFunction++;
                    bucket = fastrange64(MurmurHash64Seeded(item.key, item.currentHashFunction), this->numBuckets);

                    if (item.currentHashFunction > 100) {
                        // Empirically, making this number larger does not increase the success probability
                        // but increases the duration of failed construction attempts significantly.
                        std::cout<<std::endl;
                        std::cerr<<"Unable to insert item. Please reduce the load factor and try again."<<std::endl;
                        exit(1);
                    }
                }

                this->buckets.at(bucket).items.push_back(item);
                this->buckets.at(bucket).length += item.length + overheadPerObject;

                size_t maxSize = PageConfig::PAGE_SIZE - overheadPerPage;
                if (bucket == 0) {
                    maxSize -= sizeof(MetadataObjectType) + overheadPerObject;
                }
                if (this->buckets.at(bucket).length > maxSize) {
                    handleOverflowingBucket(bucket);
                }
            }
        }

        void handleOverflowingBucket(size_t bucket) {
            size_t maxSize = PageConfig::PAGE_SIZE - overheadPerPage;
            if (bucket == 0) {
                maxSize -= sizeof(MetadataObjectType) + overheadPerObject;
            }
            if (this->buckets.at(bucket).length <= maxSize) {
                return;
            }
            std::sort(this->buckets.at(bucket).items.begin(), this->buckets.at(bucket).items.end(),
                      [&]( const auto& lhs, const auto& rhs ) {
                          return separator(lhs.key, bucket) < separator(rhs.key, bucket);
                      });

            size_t sizeSum = 0;
            size_t i = 0;
            size_t tooLargeItemSeparator = -1;
            for (;i < this->buckets.at(bucket).items.size(); i++) {
                sizeSum += this->buckets.at(bucket).items.at(i).length + overheadPerObject;
                if (sizeSum > maxSize) {
                    tooLargeItemSeparator = separator(this->buckets.at(bucket).items.at(i).key, bucket);
                    break;
                }
            }
            assert(tooLargeItemSeparator != -1);

            sizeSum = 0;
            i = 0;
            for (;i < this->buckets.at(bucket).items.size(); i++) {
                if (separator(this->buckets.at(bucket).items.at(i).key, bucket) >= tooLargeItemSeparator) {
                    break;
                }
                sizeSum += this->buckets.at(bucket).items.at(i).length + overheadPerObject;
            }

            std::vector<Item> overflow(this->buckets.at(bucket).items.begin() + i, this->buckets.at(bucket).items.end());
            assert(this->buckets.at(bucket).items.size() == overflow.size() + i);
            assert(tooLargeItemSeparator != 0 || i == 0);

            this->buckets.at(bucket).items.resize(i);
            this->buckets.at(bucket).length = sizeSum;
            assert(separators[bucket] == 0 || tooLargeItemSeparator <= separators[bucket]);
            separators[bucket] = tooLargeItemSeparator;
            for (Item overflowedItem : overflow) {
                overflowedItem.currentHashFunction++;
                insertionQueue.push_back(overflowedItem);
            }
        }

        inline size_t findBlockToAccess(uint64_t key) {
            for (size_t hashFunctionIndex = 0; hashFunctionIndex < 100000; hashFunctionIndex++) {
                size_t bucket = fastrange64(MurmurHash64Seeded(key, hashFunctionIndex), this->numBuckets);
                numInternalProbes++;
                if (separator(key, bucket) < separators[bucket]) {
                    return bucket;
                }
            }
            std::cerr<<"Unable to find block"<<std::endl;
            return -1;
        }

    protected:
        void submitQuery(QueryHandle &handle) final {
            if (!handle.completed) {
                std::cerr<<"Used handle that did not go through awaitCompletion()"<<std::endl;
                exit(1);
            }
            handle.completed = false;
            size_t bucketIndexes[handle.keys.size()];
            numQueries += handle.keys.size();
            handle.stats.notifyStartQuery(handle.keys.size());
            for (int i = 0; i < handle.keys.size(); i++) {
                bucketIndexes[i] = findBlockToAccess(handle.keys.at(i));
            }
            handle.stats.notifyFoundBlock();
            for (int i = 0; i < handle.keys.size(); i++) {
                handle.resultPointers.at(i) = handle.ioManager->enqueueRead(
                        bucketIndexes[i] * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE);
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
                std::tuple<size_t, char *> result = findKeyWithinBlock(handle.keys.at(i), handle.resultPointers.at(i));
                handle.resultLengths.at(i) = std::get<0>(result);
                handle.resultPointers.at(i) = std::get<1>(result);
            }
            handle.stats.notifyFoundKey();
            handle.completed = true;
        }
};
