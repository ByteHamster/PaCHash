#ifndef TESTCOMPARISON_UTIL_H
#define TESTCOMPARISON_UTIL_H

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

#endif //TESTCOMPARISON_UTIL_H
