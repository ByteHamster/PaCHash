#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <cassert>
#include <functional>

#include "QueryTimer.h"
#include "IoManager.h"

struct ObjectHeader {
    uint64_t key;
    uint16_t length;
};

class VariableSizeObjectStoreConfig {
    public:
        static constexpr bool SHOW_PROGRESS = false;
        static constexpr int PROGRESS_STEPS = 1;
        using IoManager = MemoryMapIO<0>;
};

class ObjectProvider {
    public:
        /**
         * Returns the size of an object. The returned size must remain constant.
         */
        [[nodiscard]] virtual size_t getLength(uint64_t key) = 0;

        /**
         * Returns a pointer to the value of the object. This method is called lazily when writing the objects,
         * so it is not necessary for the value of all objects to be available at the same time.
         * The pointer is assumed to be valid until the next call to getValue().
         */
        [[nodiscard]] virtual const char *getValue(uint64_t key) = 0;
};

template <typename Config_ = VariableSizeObjectStoreConfig>
class VariableSizeObjectStore {
    static_assert(std::is_convertible<Config_, VariableSizeObjectStoreConfig>::value,
            "Config class must inherit from VariableSizeObjectStoreConfig");
    public:
        using Config = Config_;
        const char* filename;
        size_t numObjects = 0;
        struct QueryHandle;
    public:

        explicit VariableSizeObjectStore(const char* filename) : filename(filename) {
        }

        /**
         * Write the objects to disk.
         * @param keys The list of keys to store.
         * @param objectProvider The provider allows to access the keys and their lengths.
         */
        virtual void writeToFile(std::vector<uint64_t> &keys, ObjectProvider &objectProvider) = 0;

        /**
         * Reload the data structure from the file and construct the internal-memory data structures.
         */
        virtual void reloadFromFile() = 0;

        /**
         * Create a new handle that can execute \p batchSize queries simultaneously.
         * Multiple handles can be used to execute multiple batches simultaneously.
         * It is advisable to batch queries instead of executing them one-by-one using a QueryHandle each.
         * Query handle creation is an expensive operation and should be done before the actual queries.
         */
        virtual QueryHandle newQueryHandle(size_t batchSize) {
            QueryHandle handle(*this, numQueryHandles++);
            handle.keys.resize(batchSize);
            handle.resultLengths.resize(batchSize);
            handle.resultPointers.resize(batchSize);
             return handle;
        }

        virtual void printConstructionStats() = 0;
        virtual void printQueryStats() = 0;

        static inline void LOG(const char *step, size_t progress = -1, size_t max = -1) {
            if constexpr (Config::SHOW_PROGRESS) {
                if (step == nullptr) {
                    std::cout<<"\r"<<std::flush;
                } else if (progress == -1) {
                    std::cout<<"\r# "<<step<<std::flush;
                } else if ((progress % (max/Config::PROGRESS_STEPS + 1)) == 0 || progress == max - 1) {
                    std::cout<<"\r# "<<step<<" ("<<std::round(100.0*(double)progress/(double)max)<<"%)"<<std::flush;
                }
            }
        }
    protected:
        size_t numQueryHandles = 0;
        virtual void submitQuery(QueryHandle &handle) = 0;
        virtual void awaitCompletion(QueryHandle &handle) = 0;
    public:
        struct QueryHandle {
            VariableSizeObjectStore &owner;
            const size_t handleId;

            bool completed = true;
            std::vector<uint64_t> keys;
            std::vector<size_t> resultLengths;
            std::vector<char *> resultPointers;
            QueryTimer stats;
            std::unique_ptr<IoManager> ioManager;

            QueryHandle(VariableSizeObjectStore &owner, size_t handleId)
                : owner(owner), handleId(handleId) {
            }

            void submit() {
                owner.submitQuery(*this);
            }

            void awaitCompletion() {
                owner.awaitCompletion(*this);
            }
        };
};
