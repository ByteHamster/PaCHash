#pragma once

class StoreConfig {
    public:
        using key_t = uint64_t;
        using length_t = uint16_t;
        static constexpr length_t BLOCK_LENGTH = 4096;
        static constexpr length_t MAX_OBJECT_SIZE = 4 * BLOCK_LENGTH;
};
