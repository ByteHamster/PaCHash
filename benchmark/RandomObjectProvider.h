#pragma once

#include <cstdint>
#include <random>
#include <cstring>
#include <VariableSizeObjectStore.h>
#include <PageConfig.h>

static constexpr int EQUAL_DISTRIBUTION = 1;
static constexpr int NORMAL_DISTRIBUTION = 2;
static constexpr int EXPONENTIAL_DISTRIBUTION = 3;

class RandomObjectProvider : public ObjectProvider {
    private:
        char tempObjectContent[PageConfig::MAX_OBJECT_SIZE + 10] = {};
        const int distribution;
        const size_t averageLength;
    public:
        RandomObjectProvider(int distribution, size_t averageLength)
                : distribution(distribution), averageLength(averageLength) {
        }

        [[nodiscard]] size_t getLength(uint64_t key) final {
            size_t length = sample(key);
            assert(length <= PageConfig::MAX_OBJECT_SIZE);
            return length;
        }

        [[nodiscard]] const char *getValue(uint64_t key) final {
            size_t length = getLength(key);
            assert(length > 9);
            tempObjectContent[0] = '_';
            *reinterpret_cast<uint64_t *>(&tempObjectContent[1]) = key;
            memset(&tempObjectContent[9], static_cast<char>('A' + key % ('Z' - 'A' + 1)), length-9);
            return tempObjectContent;
        }
    private:
        [[nodiscard]] uint64_t sample(uint64_t key) const {
            if (distribution == EQUAL_DISTRIBUTION) {
                return averageLength;
            } else if (distribution == NORMAL_DISTRIBUTION) {
                // Box–Muller transform
                uint64_t hash = MurmurHash64(key);
                double U1 = (double)(hash&UINT32_MAX) / (double)UINT32_MAX;
                double U2 = (double)(hash>>UINT32_WIDTH) / (double)UINT32_MAX;
                double Z = sqrt(-2*std::log(U1))*std::cos(2*M_PI*U2);
                return static_cast<uint64_t>(std::round(2*Z + averageLength));
            } else if (distribution == EXPONENTIAL_DISTRIBUTION) {
                uint64_t hash = MurmurHash64(key);
                double U = (double)hash / (double)UINT64_MAX;
                double stretch = 0.5*averageLength;
                double lambda = 1.0;
                double expNumber = log(1-U)/(-lambda);
                return static_cast<uint64_t>(std::round((averageLength - stretch/lambda) + stretch*expNumber));
            } else {
                assert(false && "Invalid distribution");
                return 0;
            }
        }
};
