#pragma once

namespace pacthash {
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
} // Namespace pacthash
