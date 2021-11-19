#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>
#include <functional>
#include <set>
#include <iomanip>
#include <exception>

#include "QueryTimer.h"
#include "ConstructionTimer.h"
#include "IoManager.h"
#include "Util.h"

class VariableSizeObjectStore {
    public:
        ConstructionTimer constructionTimer;
        struct QueryHandle {
            StoreConfig::key_t key = 0;
            StoreConfig::length_t length = 0;
            char *resultPtr = nullptr;
            char *buffer = nullptr;
            QueryTimer stats;
            uint16_t state = 0;
            // Can be used freely by users to identify handles in the awaitAny method.
            uint64_t name = 0;

            template <typename U, typename HashFunction>
            void prepare(const U &newKey, HashFunction hashFunction) {
                static_assert(std::is_same<const U &, std::decay_t<std::tuple_element_t<0, typename function_traits<HashFunction>::arg_tuple>>>::value, "Hash function must get argument of type U");
                static_assert(std::is_same<StoreConfig::key_t, std::decay_t<typename function_traits<HashFunction>::result_type>>::value, "Hash function must return StoreConfig::key_t");
                key = hashFunction(newKey);
            }

            void prepare(const std::string &newKey) {
                key = MurmurHash64(newKey.data(), newKey.length());
            }
        };
        const char* filename;
        static constexpr size_t overheadPerObject = sizeof(StoreConfig::key_t) + sizeof(StoreConfig::length_t);
        static constexpr size_t overheadPerBlock = 2 * sizeof(StoreConfig::length_t); // Number of objects and offset
        static constexpr bool SHOW_PROGRESS = true;
        static constexpr int PROGRESS_STEPS = 32;
        struct StoreMetadata {
            char magic[32] = "Variable size object store file";
            char sizeType = sizeof(StoreConfig::length_t);
            size_t numBlocks = 0;
            StoreConfig::length_t maxSize = 0;
        };
        class BlockStorage;
    protected:
        size_t numObjects = 0;
        size_t numBlocks = 0;
        StoreConfig::length_t maxSize = 0;
        const float fillDegree;
        size_t totalPayloadSize = 0;
        int openFlags;
    public:

        explicit VariableSizeObjectStore(float fillDegree, const char* filename, int openFlags)
            : filename(filename), fillDegree(fillDegree), openFlags(openFlags) {
        }

        /**
         * Reload the data structure from the file and construct the internal-memory data structures.
         */
        virtual void reloadFromFile() = 0;

        /**
         * Space usage per block, in bits.
         */
        virtual float internalSpaceUsage() = 0;

        virtual void printConstructionStats() {
            std::cout << "External space usage: " << prettyBytes(numBlocks * StoreConfig::BLOCK_LENGTH) << std::endl;
            std::cout << "External utilization: "
                      << std::round(100.0 * totalPayloadSize / (numBlocks * StoreConfig::BLOCK_LENGTH) * 10) / 10 << "%, "
                      << "with keys: " << std::round(100.0 * (totalPayloadSize + numObjects*sizeof(uint64_t)) / (numBlocks * StoreConfig::BLOCK_LENGTH) * 10) / 10 << "%, "
                      << "with keys+length: " << std::round(100.0 * (totalPayloadSize + numObjects*(sizeof(uint64_t)+sizeof(uint16_t))) / (numBlocks * StoreConfig::BLOCK_LENGTH) * 10) / 10 << "%, "
                      << "target: " <<std::round(100*fillDegree*10)/10 << "%" << std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/numObjects<<std::endl;
        }

        inline static void LOG(const char *step, size_t progress = ~0ul, size_t max = ~0) {
            if constexpr (SHOW_PROGRESS) {
                if (step == nullptr) {
                    std::cout<<"\r\033[K"<<std::flush;
                } else if (progress == ~0ul) {
                    std::cout<<"\r\033[K# "<<step<<std::flush;
                } else if ((progress % (max/PROGRESS_STEPS + 1)) == 0 || progress == max - 1) {
                    std::cout<<"\r\033[K# "<<step<<" ("<<std::round(100.0*(double)progress/(double)max)<<"%)"<<std::flush;
                }
            }
        }

        virtual size_t requiredBufferPerQuery() = 0;
        virtual size_t requiredIosPerQuery() = 0;

        static std::tuple<StoreConfig::length_t, char *> findKeyWithinBlock(StoreConfig::key_t key, char *data) {
            BlockStorage block(data);
            char *objectPointer = block.objectsStart;
            for (size_t i = 0; i < block.numObjects; i++) {
                if (key == block.keys[i]) {
                    return std::make_tuple(block.lengths[i], objectPointer);
                }
                objectPointer += block.lengths[i];
            }
            return std::make_tuple(0, nullptr);
        }

        template <class Iterator, typename LengthExtractor, class U = typename std::iterator_traits<Iterator>::value_type>
        void printSizeHistogram(Iterator begin, Iterator end, LengthExtractor lengthExtractor) {
            static_assert(std::is_same<U, std::decay_t<std::tuple_element_t<0, typename function_traits<LengthExtractor>::arg_tuple>>>::value, "Length extractor must get argument of type U");
            static_assert(std::is_same<StoreConfig::length_t, std::decay_t<typename function_traits<LengthExtractor>::result_type>>::value, "Length extractor must return StoreConfig::length_t");
            if (begin == end) {
                std::cout<<"Empty input"<<std::endl;
                return;
            }

            std::vector<size_t> sizeHistogram(this->maxSize + 1);
            StoreConfig::length_t minSize = ~StoreConfig::length_t(0);
            StoreConfig::length_t maxSize = 0;
            size_t sum = 0;
            auto it = begin;
            while (it != end) {
                StoreConfig::length_t size = lengthExtractor(*it);
                assert(size <= this->maxSize);
                sizeHistogram.at(size)++;
                sum += size;
                minSize = std::min(size, minSize);
                maxSize = std::max(size, maxSize);
                ++it;
            }

            size_t maxLines = 100;
            size_t min = std::max(std::max(size_t(minSize), 5ul) - 5ul, 0ul);
            size_t max = std::min(maxSize + 5ul, sizeHistogram.size());
            size_t stepSize = (max-min <= maxLines) ? 1 : (max-min)/maxLines;
            size_t histogramSum = 0;
            size_t maxHistogramSum = 0;
            for (size_t i = min; i < max; i++) {
                histogramSum += sizeHistogram.at(i);
                if (i % stepSize == 0) {
                    maxHistogramSum = std::max(maxHistogramSum, histogramSum);
                    histogramSum = 0;
                }
            }
            for (size_t i = min; i < max; i++) {
                histogramSum += sizeHistogram.at(i);
                if ((i % stepSize == 0 || i == max - 1) && i != 0) {
                    std::cout <<"Size <= ";
                    std::cout << std::fixed << std::setprecision(0) << std::setw(log10(max) + 1) << std::setfill(' ');
                    std::cout << i << ":  ";
                    std::cout << std::fixed << std::setprecision(0) << std::setw(log10(maxHistogramSum) + 1) << std::setfill(' ');
                    std::cout << histogramSum << " items | "
                        << std::string(std::ceil((double) histogramSum/maxHistogramSum * 70.0), '#') << std::endl;
                    histogramSum = 0;
                }
            }
            std::cout<<"Objects: "<<(end - begin)<<std::endl;
            std::cout<<"Sizes: avg="<<sum/(end-begin)<<", min="<<minSize<<", max="<<maxSize<<std::endl;
        }

        void printSizeHistogram(std::vector<std::pair<std::string, std::string>> &vector) {
            auto lengthEx = [](const std::pair<std::string, std::string> &x) -> StoreConfig::length_t {
                return std::get<1>(x).length();
            };
            printSizeHistogram(vector.begin(), vector.end(), lengthEx);
        }

        static struct StoreMetadata readMetadata(const char *filename) {
            int fd = open(filename, O_RDONLY);
            if (fd < 0) {
                throw std::ios_base::failure("Unable to open " + std::string(filename)
                         + ": " + std::string(strerror(errno)));
            }
            char *fileFirstPage = static_cast<char *>(mmap(nullptr, StoreConfig::BLOCK_LENGTH,
                                                           PROT_READ, MAP_PRIVATE, fd, 0));
            struct StoreMetadata metadata;
            memcpy(&metadata, &fileFirstPage[0], sizeof(struct StoreMetadata));
            struct StoreMetadata defaultMetadata;
            if (memcmp(&defaultMetadata.magic, &metadata.magic, sizeof(metadata.magic)) != 0) {
                throw std::logic_error("Magic bytes do not match. Is this really an object store?");
            } else if (defaultMetadata.sizeType != metadata.sizeType) {
                throw std::logic_error("Loaded file uses " + std::to_string(metadata.sizeType)
                    + " byte lengths but this binary is compiled to use "
                    + std::to_string(defaultMetadata.sizeType) + " bytes");
            }
            munmap(fileFirstPage, StoreConfig::BLOCK_LENGTH);
            close(fd);
            return metadata;
        }

        class BlockStorage {
            public:
                char *blockStart = nullptr;
                StoreConfig::length_t offset = 0;
                StoreConfig::length_t numObjects = 0;
                char *tableStart = nullptr;
                StoreConfig::length_t *lengths = nullptr;
                StoreConfig::key_t *keys = nullptr;
                char *objectsStart = nullptr;

                explicit BlockStorage(char *data) {
                    blockStart = data;
                    memcpy(&offset, &data[StoreConfig::BLOCK_LENGTH - sizeof(StoreConfig::length_t)], sizeof(StoreConfig::length_t));
                    memcpy(&numObjects, &data[StoreConfig::BLOCK_LENGTH - 2 * sizeof(StoreConfig::length_t)], sizeof(StoreConfig::length_t));
                    tableStart = &data[StoreConfig::BLOCK_LENGTH - overheadPerBlock - numObjects * overheadPerObject];
                    lengths = reinterpret_cast<StoreConfig::length_t *>(&tableStart[numObjects * sizeof(StoreConfig::key_t)]);
                    keys = reinterpret_cast<StoreConfig::key_t *>(tableStart);
                    objectsStart = &data[offset];
                    assert(numObjects < StoreConfig::BLOCK_LENGTH);
                }

                explicit BlockStorage() = default;

                static BlockStorage init(char *data, StoreConfig::length_t offset, StoreConfig::length_t numObjects) {
                    assert(numObjects < StoreConfig::BLOCK_LENGTH);
                    memcpy(&data[StoreConfig::BLOCK_LENGTH - sizeof(StoreConfig::length_t)], &offset, sizeof(StoreConfig::length_t));
                    memcpy(&data[StoreConfig::BLOCK_LENGTH - 2 * sizeof(StoreConfig::length_t)], &numObjects, sizeof(StoreConfig::length_t));
                    return BlockStorage(data);
                }
        };
};

/**
 * Can be used to query an object store.
 * Multiple Views can be opened on one single object store to support multi-threaded queries without locking.
 */
template <class ObjectStore, class IoManager>
class ObjectStoreView {
    public:
        ObjectStore *objectStore;
        IoManager ioManager;

        ObjectStoreView(ObjectStore &objectStore, int openFlags, size_t maxSimultaneousRequests)
            : objectStore(&objectStore),
              ioManager(objectStore.filename, openFlags, maxSimultaneousRequests * objectStore.requiredIosPerQuery()) {
        }

        inline void submitSingleQuery(VariableSizeObjectStore::QueryHandle *handle) {
            objectStore->submitSingleQuery(handle, &ioManager);
        }

        inline VariableSizeObjectStore::QueryHandle *awaitAny() {
            return objectStore->awaitAny(&ioManager);
        }

        inline VariableSizeObjectStore::QueryHandle *peekAny() {
            return objectStore->peekAny(&ioManager);
        }

        inline void submitQuery(VariableSizeObjectStore::QueryHandle *handle) {
            objectStore->submitSingleQuery(handle, &ioManager);
            ioManager.submit();
        }

        inline void submit() {
            ioManager.submit();
        }
};
