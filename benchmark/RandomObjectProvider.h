#pragma once

#include <cstdint>
#include <random>
#include <cstring>
#include <VariableSizeObjectStore.h>

static constexpr int EQUAL_DISTRIBUTION = 1;
static constexpr int NORMAL_DISTRIBUTION = 2;
static constexpr int EXPONENTIAL_DISTRIBUTION = 3;

template <int distribution, int averageLength>
class RandomObjectProvider : public ObjectProvider {
    private:
        char tempObjectContent[1000] = {};
    public:
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
        static uint64_t sample(std::default_random_engine &prng) {
            if constexpr (distribution == EQUAL_DISTRIBUTION) {
                return averageLength;
            } else if constexpr (distribution == NORMAL_DISTRIBUTION) {
                std::normal_distribution<double> normalDist(averageLength, 1.0);
                return normalDist(prng);
            } else if constexpr (distribution == EXPONENTIAL_DISTRIBUTION) {
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
