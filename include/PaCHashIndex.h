#pragma once

#include "EliasFano.h"
#include "Util.h"

namespace pachash {

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
            if (iPtr > 0 && *iPtr == bin) {
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
