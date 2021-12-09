#pragma once

namespace pacthash {
constexpr uint16_t floorlog2(uint16_t x) {
    return x == 1 ? 0 : 1+floorlog2(x >> 1);
}

constexpr uint16_t ceillog2(uint16_t x) {
    return x == 1 ? 0 : floorlog2(x - 1) + 1;
}

static std::string prettyBytes(size_t bytes) {
    const char* suffixes[7];
    suffixes[0] = " B";
    suffixes[1] = " KB";
    suffixes[2] = " MB";
    suffixes[3] = " GB";
    suffixes[4] = " TB";
    suffixes[5] = " PB";
    suffixes[6] = " EB";
    uint s = 0; // which suffix to use
    double count = bytes;
    while (count >= 1024 && s < 7) {
        s++;
        count /= 1024;
    }
    return std::to_string(count) + suffixes[s];
}

#if defined(_MSC_VER) && defined (_WIN64)
#include <intrin.h>// should be part of all recent Visual Studio
#pragma intrinsic(_umul128)
#endif // defined(_MSC_VER) && defined (_WIN64)

/**
* Given a value "word", produces an integer in [0,p) without division.
* The function is as fair as possible in the sense that if you iterate
* through all possible values of "word", then you will generate all
* possible outputs as uniformly as possible.
* Source: https://github.com/lemire/fastrange/blob/master/fastrange.h
*/
static inline uint64_t fastrange64(uint64_t word, uint64_t p) {
#ifdef __SIZEOF_INT128__ // then we know we have a 128-bit int
    return (uint64_t)(((__uint128_t)word * (__uint128_t)p) >> 64);
#elif defined(_MSC_VER) && defined(_WIN64)
    // supported in Visual Studio 2005 and better
    uint64_t highProduct;
    _umul128(word, p, &highProduct); // ignore output
    return highProduct;
    unsigned __int64 _umul128(
            unsigned __int64 Multiplier,
            unsigned __int64 Multiplicand,
            unsigned __int64 *HighProduct
            );
#else
    return word % p; // fallback
#endif // __SIZEOF_INT128__
}

static inline uint64_t MurmurHash64(const void * key, int len) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const size_t seed = 1203989050u;
    const int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t * data = (const uint64_t *) key;
    const uint64_t * end = data + (len/8);

    while(data != end)
    {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char * data2 = (const unsigned char*) data;

    switch(len & 7)
    {
        case 7: h ^= uint64_t(data2[6]) << 48; // fallthrough
        case 6: h ^= uint64_t(data2[5]) << 40; // fallthrough
        case 5: h ^= uint64_t(data2[4]) << 32; // fallthrough
        case 4: h ^= uint64_t(data2[3]) << 24; // fallthrough
        case 3: h ^= uint64_t(data2[2]) << 16; // fallthrough
        case 2: h ^= uint64_t(data2[1]) << 8;  // fallthrough
        case 1: h ^= uint64_t(data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

static inline uint64_t MurmurHash64(uint64_t key) {
    return MurmurHash64(&key, sizeof(key));
}

static inline uint64_t MurmurHash64Seeded(uint64_t key, uint64_t seed) {
    return MurmurHash64(MurmurHash64(key) ^ seed);
}

class XorShift64 {
    private:
        uint64_t x64;
    public:
        explicit XorShift64(uint64_t seed = 88172645463325252ull) : x64(seed) {
        }

        inline uint64_t operator()() {
            x64 ^= x64 << 13;
            x64 ^= x64 >> 7;
            x64 ^= x64 << 17;
            return x64;
        }

        inline uint64_t operator()(uint64_t range) {
            return fastrange64(operator()(), range);
        }
};

size_t filesize(int fd) {
    struct stat st = {};
    if (fstat(fd, &st) < 0) {
        assert(false);
    }
    if (S_ISBLK(st.st_mode)) {
        uint64_t bytes;
        if (ioctl(fd, BLKGETSIZE64, &bytes) != 0) {
            assert(false);
        }
        return bytes;
    } else if (S_ISREG(st.st_mode)) {
        return st.st_size;
    }
    return 0;
}

// Workaround for select data structure crash
static size_t pastaCrashWorkaroundSize(size_t requestedSize) {
    while ((((requestedSize>>6) + 1) & 7) != 0) {
            requestedSize += 64;
        }
    return requestedSize;
}

} // Namespace pacthash
