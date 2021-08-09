#include <cstdint>
#include <random>
#include <cstring>
#include <VariableSizeObjectStore.h>

class Distribution {
    public:
        static constexpr int EQUAL = 1;
        static constexpr int NORMAL = 2;
        static constexpr int EXPONENTIAL = 3;

        template <int distribution>
        static uint64_t sample(size_t averageLength, std::default_random_engine &prng) {
            averageLength -= sizeof(ObjectHeader);
            if constexpr (distribution == Distribution::EQUAL) {
                return averageLength;
            } else if constexpr (distribution == Distribution::NORMAL) {
                std::normal_distribution<double> normalDist(averageLength, 1.0);
                return normalDist(prng);
            } else if constexpr (distribution == Distribution::EXPONENTIAL) {
                double stretch = averageLength/2;
                double lambda = 1.0;
                std::exponential_distribution<double> expDist(lambda);
                return (averageLength - stretch/lambda) + stretch*expDist(prng);
            } else {
                assert(false && "Invalid distribution");
                return 0;
            }
        }

        template <int distribution>
        static size_t lengthFor(uint64_t key, size_t averageLength) {
            std::default_random_engine generator(key);
            return Distribution::sample<distribution>(averageLength, generator);
        }

        template <int distribution>
        static std::string valueFor(uint64_t key, size_t averageLength) {
            size_t length = Distribution::lengthFor<distribution>(key, averageLength);
            assert(length > 9);
            char string[length];
            string[0] = '_';
            *reinterpret_cast<uint64_t *>(&string[1]) = key;
            memset(&string[9], '.', length-9);
            return std::string(string, length);
        }
};
