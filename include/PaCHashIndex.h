#pragma once

#include "EliasFano.h"
#include "Util.h"
#include "pasta/bit_vector/bit_vector.hpp"
#include "pasta/bit_vector/support/flat_rank_select.hpp"
#include "pasta/bit_vector/compression/block_compressed_bit_vector.hpp"
#include <la_vector.hpp>

namespace pachash {

class UncompressedBitVectorIndex {
    private:
        using RankSelect = pasta::FlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES>;
        pasta::BitVector bitVector;
        size_t numPushed = 0;
        RankSelect rankSelect;
        size_t numBlocks;
    public:
        UncompressedBitVectorIndex(size_t numBlocks, size_t numBins)
                : bitVector(numBlocks + numBins, false), numBlocks(numBlocks) {
        }

        static std::string name() {
            return "UncompressedBitVector";
        }

        inline void push_back(size_t bin) {
            bitVector[numPushed + bin] = true;
            numPushed++;
        }

        void complete() {
            rankSelect = RankSelect(bitVector);
        }

        inline void locate(size_t bin, std::tuple<size_t, size_t> &result) {
            size_t possiblePositionOfB = (bin == 0) ? 0 : (rankSelect.select0(bin) + 1);
            size_t arrayIndexOfPredecessor = (bin == 0) ? 0 : (possiblePositionOfB - bin - 1 + bitVector[possiblePositionOfB]);
            size_t bitVectorIndexOfPredecessor = rankSelect.select1(arrayIndexOfPredecessor + 1);
            size_t valueOfPredecessor = bitVectorIndexOfPredecessor - arrayIndexOfPredecessor;
            size_t i = arrayIndexOfPredecessor;
            if (valueOfPredecessor == bin && i != 0) {
                i--;
            }
            size_t j = arrayIndexOfPredecessor;
            while (bitVector[bitVectorIndexOfPredecessor + 1] && j < numBlocks - 1) {
                j++;
                bitVectorIndexOfPredecessor++;
            }
            std::get<0>(result) = i;
            std::get<1>(result) = j - i + 1;
        }

        size_t space() {
            return bitVector.data().size_bytes() + rankSelect.space_usage();
        }
};

class LaVectorIndex {
    private:
        using CompressedVector = la_vector<size_t, 0>;
        std::vector<size_t> uncompressedVector;
        CompressedVector compressedVector;
    public:
        LaVectorIndex(size_t numBlocks, size_t numBins) {
            (void) numBins;
            uncompressedVector.reserve(numBlocks);
        }

        static std::string name() {
            return "LaVectorIndex";
        }

        inline void push_back(size_t bin) {
            uncompressedVector.push_back(bin);
        }

        void complete() {
            compressedVector = CompressedVector(uncompressedVector);
        }

        inline void locate(size_t bin, std::tuple<size_t, size_t> &result) {
            auto iPtr = compressedVector.lower_bound(bin); // Successor query
            if (iPtr == compressedVector.end() || (iPtr != compressedVector.begin() && *iPtr > bin)) {
                iPtr--; // Predecessor
            }
            auto jPtr = iPtr;
            if (*iPtr == bin && iPtr != compressedVector.begin()) {
                --iPtr;
            }
            while (jPtr != compressedVector.end()) {
                auto nextPointer = jPtr;
                ++nextPointer;
                if (*nextPointer > bin) {
                    break;
                }
                jPtr = nextPointer;
            }
            std::get<0>(result) = iPtr - compressedVector.begin();
            std::get<1>(result) = jPtr - iPtr + 1;
        }

        size_t space() {
            return compressedVector.size_in_bytes();
        }
};

class CompressedBitVectorIndex {
    private:
        using RankSelect = pasta::FlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES,
                                                 pasta::FindL2FlatWith::LINEAR_SEARCH,
                                                 pasta::BlockCompressedBitVector<>>;
        pasta::BitVector bitVector;
        pasta::BlockCompressedBitVector<> *compressedBitVector = nullptr;
        size_t numPushed = 0;
        RankSelect rankSelect;
    public:
        CompressedBitVectorIndex(size_t numBlocks, size_t numBins)
                : bitVector(numBlocks + numBins, false) {
        }

        static std::string name() {
            return "CompressedBitVector";
        }

        inline void push_back(size_t bin) {
            bitVector[numPushed + bin] = true;
            numPushed++;
        }

        void complete() {
            compressedBitVector = new pasta::BlockCompressedBitVector(std::move(bitVector));
            rankSelect = RankSelect(*compressedBitVector);
        }

        inline void locate(size_t bin, std::tuple<size_t, size_t> &result) {
            size_t possiblePositionOfB = (bin == 0) ? 0 : (rankSelect.select0(bin) + 1);
            size_t arrayIndexOfPredecessor = rankSelect.rank1(possiblePositionOfB + 1) - 1;
            size_t bitVectorIndexOfPredecessor = rankSelect.select1(arrayIndexOfPredecessor + 1);
            size_t valueOfPredecessor = bitVectorIndexOfPredecessor - arrayIndexOfPredecessor;
            size_t i = arrayIndexOfPredecessor;
            if (valueOfPredecessor == bin && i != 0) {
                i--;
            }
            size_t j = rankSelect.select0(valueOfPredecessor + 1) - (valueOfPredecessor + 1);

            std::get<0>(result) = i;
            std::get<1>(result) = j - i + 1;
        }

        size_t space() {
            return compressedBitVector->space_usage() + rankSelect.space_usage();
        }
};

template <uint16_t fanoSize>
class EliasFanoIndex {
    private:
        size_t numBlocks;
        EliasFano<fanoSize> firstBinInBlockEf;
    public:
        EliasFanoIndex(size_t numBlocks, size_t numBins)
            : numBlocks(numBlocks), firstBinInBlockEf(numBlocks, numBins) {
        }

        static std::string name() {
            return "EliasFano";
        }

        inline void push_back(size_t bin) {
            firstBinInBlockEf.push_back(bin);
        }

        void complete() {
            firstBinInBlockEf.predecessorPosition(0);
        }

        inline void locate(size_t bin, std::tuple<size_t, size_t> &result) {
            auto iPtr = firstBinInBlockEf.predecessorPosition(bin);
            auto jPtr = iPtr;
            if (*iPtr == bin && iPtr > 0) {
                --iPtr;
            }
            while (jPtr < numBlocks - 1) {
                auto nextPointer = jPtr;
                ++nextPointer;
                if (*nextPointer > bin) {
                    break;
                }
                jPtr = nextPointer;
            }
            std::get<0>(result) = iPtr;
            std::get<1>(result) = jPtr - iPtr + 1;
        }

        size_t space() {
            return firstBinInBlockEf.space();
        }
};

/** For testing only */
template <typename Index1, typename Index2>
class TestingComparingIndex {
    private:
        Index1 index1;
        Index2 index2;
    public:
        TestingComparingIndex(size_t numBlocks, size_t numBins)
                : index1(numBlocks, numBins), index2(numBlocks, numBins) {
        }

        static std::string name() {
            return "TestingComparingIndex<" + Index1::name() + ", " + Index2::name() + ">";
        }

        inline void push_back(size_t bin) {
            index1.push_back(bin);
            index2.push_back(bin);
        }

        void complete() {
            index1.complete();
            index2.complete();
        }

        template <typename T>
        void assertEquals(T t1, T t2) {
            if (t1 != t2) {
                throw std::logic_error("Expected " + std::to_string(t1) + ", but got " + std::to_string(t2));
            }
        }

        inline void locate(size_t bin, std::tuple<size_t, size_t> &result) {
            index1.locate(bin, result);
            std::tuple<size_t, size_t> result2;
            index2.locate(bin, result2);
            assertEquals(std::get<0>(result), std::get<0>(result2));
            assertEquals(std::get<1>(result), std::get<1>(result2));
        }

        size_t space() {
            return 0;
        }
};

} // Namespace pachash
