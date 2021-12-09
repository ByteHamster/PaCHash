#pragma once

namespace pacthash {
class StoreConfig {
    public:
        using key_t = uint64_t;
        using length_t = uint16_t;
        #ifdef VARIABLE_SIZE_STORE_BLOCK_LENGTH
        static constexpr length_t BLOCK_LENGTH = VARIABLE_SIZE_STORE_BLOCK_LENGTH;
        #else
        static constexpr length_t BLOCK_LENGTH = 4096;
        #endif
};
} // Namespace pacthash
