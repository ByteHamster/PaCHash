#pragma once

#include "EliasFano.h"
#include "Util.h"
#include "pasta/bit_vector/bit_vector.hpp"
#include "pasta/bit_vector/support/flat_rank_select.hpp"
#include "pasta/bit_vector/compression/block_compressed_bit_vector.hpp"

namespace pachash {

class UncompressedBitVectorIndex {
    private:
        using RankSelect = pasta::FlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES>;
        pasta::BitVector bitVector;
        size_t numPushed = 0;
        pasta::FlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES> rankSelect;
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
            //size_t arrayIndexOfPredecessor = rankSelect.rank1(possiblePositionOfB + 1) - 1; // Equivalent:
            size_t arrayIndexOfPredecessor = (bin == 0) ? 0 : (possiblePositionOfB - bin - 1 + bitVector[possiblePositionOfB]);
            size_t bitVectorIndexOfPredecessor = rankSelect.select1(arrayIndexOfPredecessor + 1);
            // In practice, scanning is faster than select1
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
} // Namespace pachash
