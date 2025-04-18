#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>
#include <functional>
#include <set>
#include <iomanip>
#include <exception>
#include <bytehamster/util/Files.h>

#include "QueryTimer.h"
#include "ConstructionTimer.h"
#include "IoManager.h"
#include "ObjectStoreView.h"
#include "Log.h"

namespace pachash {
class VariableSizeObjectStore {
    public:
        ConstructionTimer constructionTimer;
        const char* filename;
        static constexpr size_t overheadPerObject = sizeof(StoreConfig::key_t) + sizeof(StoreConfig::offset_t);
        static constexpr size_t overheadPerBlock = sizeof(StoreConfig::num_objects_t) + sizeof(char); // num+emptyPageEnd
        struct StoreMetadata {
            static constexpr uint16_t TYPE_PACHASH = 1000;
            static constexpr uint16_t TYPE_SEPARATOR = 2000;
            static constexpr uint16_t TYPE_CUCKOO = 0;
            char magic[32] = "Variable size object store file";
            char version = 1;
            uint16_t type = 1;
            size_t numBlocks = 0;
            size_t maxSize = 0;
        };
        class BlockStorage;
    protected:
        size_t numObjects = 0;
        size_t numBlocks = 0;
        size_t maxSize = 0;
        const float loadFactor;
        size_t totalPayloadSize = 0;
        int openFlags;
    public:

        explicit VariableSizeObjectStore(float loadFactor, const char* filename, int openFlags)
            : filename(filename), loadFactor(loadFactor), openFlags(openFlags) {
        }

        virtual ~VariableSizeObjectStore() = default;

        /**
         * Reload the data structure from the file and construct the internal-memory data structures.
         */
        virtual void buildIndex() = 0;

        /**
         * Space usage per block, in bits.
         */
        virtual float internalSpaceUsage() = 0;

        virtual void printConstructionStats() {
            std::cout << "External space usage: "
                << bytehamster::util::prettyBytes(numBlocks * StoreConfig::BLOCK_LENGTH) << std::endl;
            size_t round = 1000;
            std::cout << "External utilization: only data: "
                      << std::round(100.0 * totalPayloadSize / (numBlocks * StoreConfig::BLOCK_LENGTH) * round) / round << "%, "
                      << "with keys: " << std::round(100.0 * (totalPayloadSize + numObjects*sizeof(uint64_t))
                            / (numBlocks * StoreConfig::BLOCK_LENGTH) * round) / round << "%, " << "with keys+length: "
                      << std::round(100.0 * (totalPayloadSize
                                + numObjects*(sizeof(StoreConfig::offset_t)+sizeof(StoreConfig::key_t)))
                            / (numBlocks * StoreConfig::BLOCK_LENGTH) * round) / round << "%, "
                      << "target: " << std::round(100 * loadFactor * round) / round << "%" << std::endl;
            std::cout<<"Average object payload size: "<<(double)totalPayloadSize/numObjects<<std::endl;
        }

        virtual size_t requiredBufferPerQuery() = 0;
        virtual size_t requiredIosPerQuery() = 0;

        /**
         * Table layout for methods that do not have overlapping objects is shifted:
         * Object 0 always starts at position 0. Object i starts at offset[i-1].
         * offset[num - 1] stores the end of the last object.
         * The length can be calculated by subtracting objects.
         */
        static std::tuple<size_t, char *> findKeyWithinNonOverlappingBlock(StoreConfig::key_t key, char *data) {
            BlockStorage block(data);
            for (size_t i = 0; i < block.numObjects; i++) {
                if (key == block.keys[i]) {
                    if (i == 0) {
                        return std::make_tuple(
                                block.offsets[0], // Size
                                block.blockStart); // Pointer
                    } else {
                        return std::make_tuple(
                                block.offsets[i] - block.offsets[i - 1], // Size
                                block.blockStart + block.offsets[i - 1]); // Pointer
                    }
                }
            }
            return std::make_tuple(0, nullptr);
        }

        template <class Iterator, typename LengthExtractor, class U = typename std::iterator_traits<Iterator>::value_type>
        static void printSizeHistogram(Iterator begin, Iterator end, LengthExtractor lengthExtractor) {
            static_assert(std::is_invocable_r_v<size_t, LengthExtractor, U>);
            if (begin == end) {
                std::cout<<"Empty input"<<std::endl;
                return;
            }
            std::ios cout_state(nullptr);
            cout_state.copyfmt(std::cout);

            size_t sum = 0;
            size_t maxSize = 0;
            size_t minSize = ~0ull;
            auto it = begin;
            while (it != end) {
                size_t size = lengthExtractor(*it);
                minSize = std::min(size, minSize);
                maxSize = std::max(size, maxSize);
                sum += size;
                ++it;
            }

            std::vector<size_t> sizeHistogram(maxSize + 1);
            it = begin;
            while (it != end) {
                sizeHistogram.at(lengthExtractor(*it))++;
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
                    std::cout << std::fixed << std::setprecision(0)
                            << std::setw(log10(max) + 1) << std::setfill(' ');
                    std::cout << i << ":  ";
                    std::cout << std::fixed << std::setprecision(0)
                            << std::setw(log10(maxHistogramSum) + 1) << std::setfill(' ');
                    std::cout << histogramSum << " items | " << std::string(
                            std::ceil((double) histogramSum/maxHistogramSum * 70.0), '#') << std::endl;
                    histogramSum = 0;
                }
            }
            std::cout<<"Objects: "<<(end - begin)<<std::endl;
            std::cout<<"Sizes: avg="<<sum/(end-begin)<<", min="<<minSize<<", max="<<maxSize<<std::endl;

            size_t numItems = 0;
            for (size_t i = 0; i < sizeHistogram.size(); i++) {
                numItems += sizeHistogram.at(i);
                if (numItems >= size_t(end-begin)/2) {
                    std::cout<<"Median: "<<i<<std::endl;
                    break;
                }
            }
            std::cout.copyfmt(cout_state);
        }

        static void printSizeHistogram(std::vector<std::pair<std::string, std::string>> &vector) {
            auto lengthEx = [](const std::pair<std::string, std::string> &x) -> size_t {
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
            } else if (defaultMetadata.version != metadata.version) {
                throw std::logic_error("Loaded file is version " + std::to_string(metadata.version)
                    + " but this binary supports only version " + std::to_string(defaultMetadata.version));
            }
            munmap(fileFirstPage, StoreConfig::BLOCK_LENGTH);
            close(fd);
            return metadata;
        }

        class BlockStorage {
            public:
                char *blockStart = nullptr;
                StoreConfig::num_objects_t numObjects = 0;
                char emptyPageEnd = 0;
                char *tableStart = nullptr;
                StoreConfig::offset_t *offsets = nullptr;
                StoreConfig::key_t *keys = nullptr;

                explicit BlockStorage(char *data) {
                    blockStart = data;
                    memcpy(&numObjects, &data[StoreConfig::BLOCK_LENGTH - sizeof(StoreConfig::num_objects_t)],
                           sizeof(StoreConfig::num_objects_t));
                    emptyPageEnd = data[StoreConfig::BLOCK_LENGTH - overheadPerBlock];
                    tableStart = &data[StoreConfig::BLOCK_LENGTH - overheadPerBlock - numObjects * overheadPerObject];
                    offsets = reinterpret_cast<StoreConfig::offset_t *>(&tableStart[numObjects * sizeof(StoreConfig::key_t)]);
                    keys = reinterpret_cast<StoreConfig::key_t *>(tableStart);
                    assert(numObjects < StoreConfig::BLOCK_LENGTH);
                }

                explicit BlockStorage() = default;

                static BlockStorage init(char *data, StoreConfig::num_objects_t numObjects, char emptyPageLength = 0) {
                    assert(numObjects < StoreConfig::BLOCK_LENGTH);
                    memcpy(&data[StoreConfig::BLOCK_LENGTH - sizeof(StoreConfig::num_objects_t)], &numObjects,
                           sizeof(StoreConfig::num_objects_t));
                    data[StoreConfig::BLOCK_LENGTH - overheadPerBlock] = emptyPageLength;
                    return BlockStorage(data);
                }
        };
};
} // Namespace pachash
