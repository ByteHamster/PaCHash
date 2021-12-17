#pragma once

#include "VariableSizeObjectStore.h"

namespace pachash {
struct QueryHandle {
    StoreConfig::key_t key = 0;
    size_t length = 0;
    char *resultPtr = nullptr;
    char *buffer = nullptr;
    QueryTimer stats;
    uint16_t state = 0;
    // Can be used freely by users to identify handles returned by the awaitAny method.
    uint64_t name = 0;

    template <class ObjectStore>
    explicit QueryHandle(ObjectStore &objectStore) {
        buffer = new (std::align_val_t(pachash::StoreConfig::BLOCK_LENGTH)) char[objectStore.requiredBufferPerQuery()];
    }

    ~QueryHandle() {
        delete[] buffer;
    }

    template <typename U, typename HashFunction>
    void prepare(const U &newKey, HashFunction hashFunction) {
        static_assert(std::is_invocable_r_v<StoreConfig::key_t, HashFunction, U>);
        key = hashFunction(newKey);
    }

    void prepare(const std::string &newKey) {
        key = MurmurHash64(newKey.data(), newKey.length());
    }
};
} // Namespace pachash
