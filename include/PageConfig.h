#ifndef TESTCOMPARISON_PAGECONFIG_H
#define TESTCOMPARISON_PAGECONFIG_H


class PageConfig {
    public:
        using offset_t = uint16_t;
        static constexpr size_t PAGE_SIZE = 4096;
        static constexpr size_t MAX_OBJECT_SIZE = 4096;
        static constexpr size_t MAX_SIMULTANEOUS_QUERIES = 10;
};


#endif //TESTCOMPARISON_PAGECONFIG_H
