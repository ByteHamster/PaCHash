#pragma once

#include <vector>
#include <cassert>
#include <sdsl/bit_vectors.hpp>
#include <container/bit_vector.hpp>

template <int c>
class EliasFano {
    static_assert(c > 0);
    private:
        sdsl::int_vector<c> L;
        pasta::BitVector H;
        size_t count = 0;
        pasta::BitVectorRankSelect *rankSelect = nullptr;
        uint64_t previousInsert = 0;
        static constexpr uint64_t MASK_LOWER_BITS = ((1 << c) - 1);
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
                EliasFano<c> *fano;
            public:
                ElementPointer(size_t h, size_t positionH, size_t positionL, EliasFano<c> &fano)
                        : h(h), positionH(positionH), positionL(positionL), fano(&fano) {
                    assert(fano.H[positionH] == 1);
                }

                ElementPointer& operator=(const ElementPointer &other) {
                    h = other.h;
                    positionL = other.positionL;
                    positionH = other.positionH;
                    fano = other.fano;
                    return *this;
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
                    uint64_t l = static_cast<const sdsl::int_vector<c>&>(fano->L)[positionL];
                    return (h << c) + l;
                }

                operator size_t() {
                    return positionL;
                }
        };

        ~EliasFano() {
            invalidateSelectDatastructure();
        }

        EliasFano(size_t num, uint64_t universeSize)
                : L(num), H(sizeWorkaround((universeSize >> c) + num + 1), false) {
            if (abs(log2((double) num) - (log2(universeSize) - c)) > 1) {
                std::cerr<<"Warning: Poor choice of bits for EF construction"<<std::endl;
                std::cerr<<"Universe: "<<universeSize<<std::endl;
                std::cerr<<"Should be roughly "<<log2(universeSize) - log2((double) num)<<std::endl;
            }
        }

        // Workaround for select data structure crash
        static size_t sizeWorkaround(size_t requestedSize) {
            while ((((requestedSize>>6) + 1) & 7) != 0) {
                requestedSize += 64;
            }
            return requestedSize;
        }

        /**
         * Each index MUST be added exactly once but they can be added without ordering.
         * Either push_back OR add can be called. Combining them is not supported.
         */
        void add(size_t index, uint64_t element) {
            uint64_t l = element & ((1l<<c) - 1);
            uint64_t h = element >> c;
            assert(element == h*(1l<<c) + l);
            L[index] = l;
            if (H.size() < h + index + 1) {
                H.resize(h + index + 1);
                std::cerr<<"Resize not supported yet"<<std::endl;
                exit(0);
            }
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
                rankSelect = new pasta::BitVectorRankSelect(H);
            }

            const uint64_t elementH = element >> c;
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
            } else {
                // Look through elements with the same upper bits
                while (true) {
                    const uint64_t lower = static_cast<const sdsl::int_vector<c>&>(L)[positionL];
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

        uint64_t at(int position) {
            if (rankSelect == nullptr) {
                rankSelect = new pasta::BitVectorRankSelect(H);
            }
            uint64_t l = static_cast<const sdsl::int_vector<c>&>(L)[position];
            uint64_t h = rankSelect->select1(position + 1) - position;
            return (h << c) + l;
        }

        [[nodiscard]] int space() {
            return L.capacity()/8 + H.size()/8 + selectStructureOverhead();
        }

        [[nodiscard]] int selectStructureOverhead() {
            return rankSelect->space_usage();
        }
};

void eliasFanoTest() {
    for (int run = 0; run < 2000; run++) {
        std::vector<uint32_t> vec;

        unsigned int seed = std::chrono::duration_cast<std::chrono::microseconds>
                (std::chrono::system_clock::now().time_since_epoch()).count();
        std::cout << seed << std::endl;
        srand(seed);
        int num = (rand() % (256 << (rand() % 12))) + 8;
        uint32_t sum = 0;
        EliasFano<3> ef(num, 5 * num);
        for (int i = 0; i < num; i++) {
            sum += random() % 5;
            vec.push_back(sum);
            ef.push_back(sum);
        }
        std::cout << "Vector:\t\t\t\t\t\t" << vec.size() * sizeof(vec.at(0)) << " bytes" << std::endl;
        std::cout << "Elias Fano basic:\t\t\t" << ef.space() << " bytes" << std::endl;

        for (int i = 0; i < num; i++) {
            if (vec.at(i) != ef.at(i)) {
                std::cerr << "Error: Does not equal at "<<i<<". Expected " << vec.at(i) << ", got " << ef.at(i) << std::endl;
                assert(false && "Error: Does not equal");
                return;
            }
        }
        auto ptr = ef.predecessorPosition(vec.at(0));
        for (int i = 0; i < num; i++) {
            assert(ptr == i);
            if (vec.at(i) != *ptr) {
                std::cerr << "Error: Increment does not equal at "<<i<<". Expected " << vec.at(i) << ", got " << *ptr << std::endl;
                assert(false && "Error: Increment does not equal");
                return;
            }
            if (i < num-1) {
                ++ptr;
            }
        }
        for (int i = num - 1; i >= 0; i--) {
            assert(ptr == i);
            if (vec.at(i) != *ptr) {
                std::cerr << "Error: Decrement does not equal at "<<i<<". Expected " << vec.at(i) << ", got " << *ptr << std::endl;
                assert(false && "Error: Decrement does not equal");
                return;
            }
            if (i > 0) {
                --ptr;
            }
        }
        std::cout << "Elias Fano with select1:\t" << ef.space() << " bytes" << std::endl;
        ef.invalidateSelectDatastructure();

        int currentPredecessor = -1;
        int i = vec.at(0);
        while (i < sum && currentPredecessor < (int) vec.size()) {
            while (i >= vec.at(currentPredecessor + 1) && (currentPredecessor == -1 || i != vec.at(currentPredecessor))) {
                currentPredecessor++;
            }
            int got = ef.predecessorPosition(i);
            if (currentPredecessor != got && currentPredecessor != -1) {
                std::cout << "Pred(" << i << ")=" << currentPredecessor
                        << ", vec(" << currentPredecessor-1 << ")="
                            << ((currentPredecessor > 0) ? (int) vec.at(currentPredecessor-1) : -1)
                        << ", vec(" << currentPredecessor << ")="
                            << ((currentPredecessor >= 0) ? (int) vec.at(currentPredecessor) : -1)
                        << ", vec(" << currentPredecessor+1 << ")="
                            << ((currentPredecessor+1 < vec.size()) ? (int) vec.at(currentPredecessor+1) : -1) << std::endl;
                std::cout << "But got: " << got << std::endl;
                std::cerr << "Error: Predecessors wrong" << std::endl;
                return;
            }
            i++;
        }
        std::cout << "Elias Fano with select0:\t" << ef.space() << " bytes" << std::endl << std::endl;
    }
}
