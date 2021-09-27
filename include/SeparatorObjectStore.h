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
            return "SeparatorObjectStore s=" + std::to_string(separatorBits);
        }

        void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) final {
            constructionTimer.notifyStartConstruction();
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
            constructionTimer.notifyDeterminedSpace();

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

            constructionTimer.notifyPlacedObjects();
            this->writeBuckets(objectProvider, false);
            constructionTimer.notifyWroteObjects();
        }

        void reloadFromFile() final {
            constructionTimer.notifySyncedFile();
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
            constructionTimer.notifyReadComplete();
        }

        float internalSpaceUsage() final {
            return (double)separatorBits/this->fillDegree;
        }

        void printConstructionStats() final {
            Super::printConstructionStats();
            std::cout<<"RAM space usage: "
                     <<prettyBytes(separators.capacity()/8)<<" ("<<separatorBits<<" bits/block, scaled: "
                     <<internalSpaceUsage()<<" bits/block)"<<std::endl;
        }

        size_t requiredBufferPerQuery() override {
            return PageConfig::PAGE_SIZE;
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
            size_t bucket = findBlockToAccess(handle->key);
            handle->stats.notifyFoundBlock();
            ioManager->enqueueRead(handle->buffer, bucket * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE,
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
            std::tuple<size_t, char *> result = findKeyWithinBlock(handle->key, handle->buffer);
            handle->length = std::get<0>(result);
            handle->resultPtr = std::get<1>(result);
            handle->stats.notifyFoundKey();
            handle->state = 0;
        }
};
