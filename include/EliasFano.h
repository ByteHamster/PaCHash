#pragma once

#include <vector>
#include <cassert>
#include <sdsl/bit_vectors.hpp>
#include <bit_vector/bit_vector.hpp>
#include <bit_vector/support/bit_vector_flat_rank_select.hpp>
#include "Util.h"

namespace pachash {
template <int lowerBits>
class EliasFano {
    static_assert(lowerBits >= 0);
    private:
        sdsl::int_vector<lowerBits> L;
        pasta::BitVector H;
        size_t count = 0;
        size_t universeSize = 0;
        pasta::BitVectorFlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES> *rankSelect = nullptr;
        uint64_t previousInsert = 0;
        static constexpr uint64_t MASK_LOWER_BITS = ((1 << lowerBits) - 1);
    public:

        /**
         * Efficient pointer into an Elias-Fano coded sequence.
         * When incrementing/decrementing and reading, no additional select query is performed.
         */
        struct ElementPointer {
            private:
                size_t positionL;
                size_t positionH;
                size_t h;
                EliasFano<lowerBits> *fano;
            public:
                ElementPointer(size_t h, size_t positionH, size_t positionL, EliasFano<lowerBits> &fano)
                        : positionL(positionL), positionH(positionH), h(h), fano(&fano) {
                    assert(fano.H[positionH] == 1);
                }

                ElementPointer& operator++() {
                    if (positionL >= fano->count - 1) {
                        // Incremented more than the number of elements in the sequence.
                        // Dereferencing it now is undefined behavior but decrementing again makes it usable again.
                        positionL++;
                        return *this;
                    }
                    assert(fano->H[positionH] == 1);
                    positionL++;
                    positionH++;
                    while (fano->H[positionH] == 0) {
                        positionH++;
                        h++;
                    }
                    assert(fano->H[positionH] == 1);
                    return *this;
                }

                ElementPointer& operator--() {
                    if (positionL >= fano->count) {
                        // Was incremented more than the number of elements in the sequence.
                        // Will be dereferenceable again if decremented to be inside the bounds.
                        positionL--;
                        return *this;
                    }
                    assert(positionL > 0);
                    assert(fano->H[positionH] == 1);
                    positionL--;
                    positionH--;
                    while (positionH > 0 && fano->H[positionH] == 0) {
                        positionH--;
                        h--;
                    }
                    assert(fano->H[positionH] == 1);
                    return *this;
                }

                uint64_t operator *() {
                    assert(positionL < fano->count);
                    if constexpr(lowerBits == 0) {
                        return h;
                    }
                    uint64_t l = static_cast<const sdsl::int_vector<lowerBits>&>(fano->L)[positionL];
                    return (h << lowerBits) + l;
                }

                operator size_t() {
                    return positionL;
                }
        };

        ~EliasFano() {
            invalidateSelectDatastructure();
        }

        EliasFano(size_t num, uint64_t universeSize)
                : L(lowerBits == 0 ? 0 : num), H((universeSize >> lowerBits) + num + 1, false),
                  universeSize(universeSize) {
            if (abs(log2((double) num) - (log2(universeSize) - lowerBits)) > 1) {
                std::cerr<<"Warning: Poor choice of bits for EF construction"<<std::endl;
                std::cerr<<"Universe: "<<universeSize<<std::endl;
                std::cerr<<"Should be roughly "<<log2(universeSize) - log2((double) num)<<std::endl;
            }
        }

        /**
         * Each index MUST be added exactly once but they can be added without ordering.
         * Either push_back OR add can be called. Combining them is not supported.
         */
        void add(size_t index, uint64_t element) {
            assert(index < L.size() || lowerBits == 0);
            assert(element < universeSize);
            uint64_t l = element & ((1l << lowerBits) - 1);
            uint64_t h = element >> lowerBits;
            assert(element == h*(1l << lowerBits) + l);
            if constexpr (lowerBits != 0) {
                L[index] = l;
            }
            assert(h + index < H.size());
            H[h + index] = true;
            invalidateSelectDatastructure();
            count++;
        }

        void push_back(uint64_t element) {
            #ifndef NDEBUG
                assert(element >= previousInsert);
                previousInsert = element;
            #endif
            add(count, element);
            //assert(at(count - 1) == element); // Very inefficient because it builds a whole select data structure
        }

        void invalidateSelectDatastructure() {
            delete rankSelect;
        }

        /**
         * Returns an ElementPointer to the last stored element that is <= the parameter.
         * When multiple duplicate elements are stored, returns the first occurrence.
         */
        ElementPointer predecessorPosition(uint64_t element) {
            assert(element >= at(0));
            if (rankSelect == nullptr) {
                rankSelect = new pasta::BitVectorFlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES>(H);
            }

            const uint64_t elementH = element >> lowerBits;
            const uint64_t elementL = element & MASK_LOWER_BITS;
            uint64_t positionH;
            uint64_t positionL;
            if (elementH == 0) {
                positionH = 0;
                positionL = 0;
            } else {
                positionH = rankSelect->select0(elementH) + 1;
                positionL = positionH - elementH;
            }
            if (H[positionH] == 0) {
                // No item with same upper bits stored
                if (positionL > 0) {
                    // Return previous item
                    positionL--;
                    positionH--; // positionH >= positionL, so no underflow
                }
            } else if constexpr (lowerBits != 0) {
                // Look through elements with the same upper bits
                while (true) {
                    const uint64_t lower = static_cast<const sdsl::int_vector<lowerBits>&>(L)[positionL];
                    if (lower > elementL) {
                        // Return previous item
                        if (positionL > 0) {
                            positionL--;
                            positionH--; // positionH >= positionL, so no underflow
                        }
                        break;
                    } else if (lower == elementL) {
                        // Return first equal item
                        break;
                    } else if (H[positionH + 1] == 0) {
                        // End of section. Next item will be larger, so return this.
                        break;
                    }
                    positionH++;
                    positionL++;
                }
            }
            // In case we returned the last item of the previous block, we need to find out its upper bits.
            uint64_t resultH = elementH;
            while (positionH > 0 && H[positionH] == 0) {
                positionH--;
                resultH--;
            }
            assert(at(positionL) <= element);
            assert(positionL == count - 1 || at(positionL + 1) >= element);
            assert(positionL == 0 || at(positionL - 1) < element);

            ElementPointer ptr(resultH, positionH, positionL, *this);
            #ifndef NDEBUG
                assert(*ptr <= element);
                if (positionL < count - 1) {
                    ++ptr;
                    assert(*ptr >= element);
                    --ptr;
                }
                if (positionL > 0) {
                    --ptr;
                    assert(*ptr < element);
                    ++ptr;
                }
            #endif
            return ptr;
        }

        ElementPointer begin() {
            size_t h = 0;
            size_t positionH = 0;
            while (H[positionH] == 0) {
                positionH++;
                h++;
            }
            return ElementPointer(h, positionH, 0, *this);
        }

        uint64_t at(int position) {
            if (rankSelect == nullptr) {
                rankSelect = new pasta::BitVectorFlatRankSelect<pasta::OptimizedFor::ZERO_QUERIES>(H);
            }
            uint64_t l = lowerBits == 0 ? 0 : static_cast<const sdsl::int_vector<lowerBits>&>(L)[position];
            uint64_t h = rankSelect->select1(position + 1) - position;
            return (h << lowerBits) + l;
        }

        [[nodiscard]] int space() {
            return L.capacity()/8 + H.size()/8 + selectStructureOverhead();
        }

        [[nodiscard]] int selectStructureOverhead() {
            return rankSelect->space_usage();
        }
};

} // Namespace pachash
