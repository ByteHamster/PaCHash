#pragma once

#include <cstdint>
#include <random>
#include <cstring>
#include <VariableSizeObjectStore.h>

static constexpr int EQUAL_DISTRIBUTION = 1;
static constexpr int NORMAL_DISTRIBUTION = 2;
static constexpr int EXPONENTIAL_DISTRIBUTION = 3;

class RandomObjectProvider : public ObjectProvider {
    private:
        char tempObjectContent[1000] = {};
        const int distribution;
        const int averageLength;
    public:
        RandomObjectProvider(int distribution, int averageLength)
                : distribution(distribution), averageLength(averageLength) {
        }

        [[nodiscard]] size_t getLength(uint64_t key) final {
            std::default_random_engine generator(key);
            return sample(generator);
        }

        [[nodiscard]] const char *getValue(uint64_t key) final {
            size_t length = getLength(key);
            assert(length > 9);
            tempObjectContent[0] = '_';
            *reinterpret_cast<uint64_t *>(&tempObjectContent[1]) = key;
            memset(&tempObjectContent[9], '.', length-9);
            return tempObjectContent;
        }
    private:
        uint64_t sample(std::default_random_engine &prng) const {
            if (distribution == EQUAL_DISTRIBUTION) {
                return averageLength;
            } else if (distribution == NORMAL_DISTRIBUTION) {
                std::normal_distribution<double> normalDist(averageLength, 1.0);
                return normalDist(prng);
            } else if (distribution == EXPONENTIAL_DISTRIBUTION) {
                double stretch = averageLength/2;
                double lambda = 1.0;
                std::exponential_distribution<double> expDist(lambda);
                return (averageLength - stretch/lambda) + stretch*expDist(prng);
            } else {
                assert(false && "Invalid distribution");
                return 0;
            }
        }
};
