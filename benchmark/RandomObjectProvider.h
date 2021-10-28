#pragma once

#include <cstdint>
#include <random>
#include <cstring>
#include <VariableSizeObjectStore.h>
#include <StoreConfig.h>

class RandomObjectProvider : public ObjectProvider {
    private:
        static constexpr int EQUAL_DISTRIBUTION = 1;
        static constexpr int NORMAL_DISTRIBUTION = 6;
        static constexpr int EXPONENTIAL_DISTRIBUTION = 3;
        static constexpr int UNIFORM_DISTRIBUTION = 4;
        static constexpr int ZIPF_DISTRIBUTION = 5;

        char tempObjectContent[StoreConfig::MAX_OBJECT_SIZE + 10] = {};
        const int distribution;
        const size_t averageLength;
        const size_t N;
    public:
        RandomObjectProvider(std::string distribution, size_t N, StoreConfig::length_t averageLength)
                : distribution(findDist(distribution)), averageLength(averageLength), N(N) {
        }

        [[nodiscard]] StoreConfig::length_t getLength(StoreConfig::key_t key) final {
            StoreConfig::length_t length = sample(key);
            assert(length <= StoreConfig::MAX_OBJECT_SIZE);
            assert(length > 9);
            return length;
        }

        [[nodiscard]] const char *getValue(StoreConfig::key_t key) final {
            StoreConfig::length_t length = getLength(key);
            assert(length > 9);
            tempObjectContent[0] = '_';
            *reinterpret_cast<StoreConfig::key_t *>(&tempObjectContent[1]) = key;
            const size_t written = 1 + sizeof(StoreConfig::key_t);
            memset(&tempObjectContent[written], static_cast<char>('A' + key % ('Z' - 'A' + 1)), length-written);
            return tempObjectContent;
        }

        static std::vector<std::pair<std::string, int>> getDistributions() {
            std::vector<std::pair<std::string, int>> distributions;
            distributions.emplace_back("equal", EQUAL_DISTRIBUTION);
            distributions.emplace_back("normal", NORMAL_DISTRIBUTION);
            distributions.emplace_back("exponential", EXPONENTIAL_DISTRIBUTION);
            distributions.emplace_back("uniform", UNIFORM_DISTRIBUTION);
            distributions.emplace_back("zipf", ZIPF_DISTRIBUTION);
            return distributions;
        }

        static std::string getDistributionsString() {
            std::string result;
            auto distributions = getDistributions();
            for (auto &pair : distributions) {
                result += pair.first + " ";
            }
            return result;
        }

        static int findDist(std::string &distribution) {
            auto distributions = getDistributions();
            for (auto &pair : distributions) {
                if (pair.first == distribution) {
                    return pair.second;
                }
            }
            std::cerr<<"Distribution "<<distribution<<" not supported. Possible values: "<<getDistributionsString()<<std::endl;
            exit(1);
            return 0;
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
                double variance = 0.5*averageLength;
                return static_cast<uint64_t>(std::max(10.0, std::min(1.0*StoreConfig::MAX_OBJECT_SIZE, std::round(variance * Z + averageLength))));
            } else if (distribution == EXPONENTIAL_DISTRIBUTION) {
                uint64_t hash = MurmurHash64(key);
                double U = (double)hash / (double)UINT64_MAX;
                double stretch = 0.5*averageLength;
                double lambda = 1.0;
                double expNumber = log(1-U)/(-lambda);
                return static_cast<uint64_t>(std::round((averageLength - stretch/lambda) + stretch*expNumber));
            } else if (distribution == UNIFORM_DISTRIBUTION) {
                uint64_t hash = MurmurHash64(key);
                double U = (double)hash / (double)UINT64_MAX;
                return static_cast<uint64_t>(10.0 + std::round((averageLength - 10.0) * U));
            } else if (distribution == ZIPF_DISTRIBUTION) {
                uint64_t hash = MurmurHash64(key);
                double U = (double)hash / (double)UINT64_MAX;
                return static_cast<uint64_t>(10.0 + approximateZipf(U, 2, N));
            } else {
                assert(false && "Invalid distribution");
                return 0;
            }
        }

        // Source: https://jasoncrease.medium.com/zipf-54912d5651cc
        static inline uint64_t approximateZipf(double p, double s, double N) {
            assert(p >= 0 && p <= 1);
            double tolerance = 0.01;
            double x = N / 2.0;

            double pD = p * (12 * (std::pow(N, -s + 1) - 1) / (1 - s) + 6 + 6 * std::pow(N, -s) + s - s * std::pow(N, -s - 1));

            while (true) {
                double m = std::pow(x, -s - 2); // x ^ ( -s - 2)
                double mx = m * x;                 // x ^ ( -s - 1)
                double mxx = mx * x;               // x ^ ( -s)
                double mxxx = mxx * x;             // x ^ ( -s + 1)

                double a = 12 * (mxxx - 1) / (1 - s) + 6 + 6 * mxx + s - (s * mx) - pD;
                double b = 12 * mxx - (6 * s * mx) + (m * s * (s + 1));
                double newx = std::max(1.0, x - a / b);
                if (std::abs(newx - x) <= tolerance) {
                    return std::round(newx);
                }
                x = newx;
            }
        }
};
