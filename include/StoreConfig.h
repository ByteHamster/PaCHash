#pragma once

// Measure timing steps for every single query.
// This slows down the measurements by about 5-10%
//#define MEASURE_QUERY_TIMING

// Show progress bars while executing queries/constructing.
// This slows down the measurements slightly.
#ifndef NDEBUG
#define LOGGING_ENABLED
#endif

namespace pachash {
class StoreConfig {
    public:
        using key_t = uint64_t;
        using offset_t = uint16_t;
        using num_objects_t = uint16_t;
        #ifdef VARIABLE_SIZE_STORE_BLOCK_LENGTH
        static constexpr offset_t BLOCK_LENGTH = VARIABLE_SIZE_STORE_BLOCK_LENGTH;
        #else
        static constexpr offset_t BLOCK_LENGTH = 4096;
        #endif
};
} // Namespace pachash
