#include <PaCHashObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <SeparatorObjectStore.h>
#include <LinearObjectReader.h>
#include <tlx/cmdline_parser.hpp>
#include <Util.h>

std::string storeFile = "key_value_store.db";
std::string indexType = "eliasFano";
size_t numQueries = 1000;
bool useCachedIo = false;
std::vector<pachash::StoreConfig::key_t> keys;
size_t pachashParameterA = 8;
constexpr uint8_t separatorBits = 6;

/**
 * This more complex example submits multiple queries to be kept in-flight at the same time.
 * If a query is completed, it prepares a new one. If there is no query to process, it submits the prepared ones.
 * The keys for the queries are read from the input file when starting.
 */
 template <typename ObjectStore>
 void performQueries() {
     size_t numKeys = keys.size();
     size_t depth = 128;
     ObjectStore objectStore(1.0, storeFile.c_str(), useCachedIo ? 0 : O_DIRECT);
     objectStore.buildIndex();

     pachash::ObjectStoreView<ObjectStore, pachash::UringIO> objectStoreView(objectStore, useCachedIo ? 0 : O_DIRECT, depth);
     std::vector<pachash::QueryHandle> queryHandles;
     queryHandles.reserve(depth);
     for (size_t i = 0; i < depth; i++) {
         queryHandles.emplace_back(objectStore);
     }

     pachash::XorShift64 prng(time(nullptr));
     // Accessed linearly at query time, while `keys` array would be accessed randomly
     std::vector<pachash::StoreConfig::key_t> keyQueryOrder;
     keyQueryOrder.reserve(numQueries + depth);
     for (size_t i = 0; i < numQueries + depth; i++) {
         keyQueryOrder.push_back(keys.at(prng(numKeys)));
         pachash::LOG("Preparing list of keys to query", i, numQueries);
     }

     // Fill in-flight queue
     for (size_t i = 0; i < depth; i++) {
         queryHandles[i].key = keyQueryOrder[i];
         objectStoreView.enqueueQuery(&queryHandles[i]);
     }
     objectStoreView.submit();

     size_t handled = 0;
     auto queryStart = std::chrono::high_resolution_clock::now();
     // Submit new queries as old ones complete
     while (handled < numQueries) {
         pachash::QueryHandle *handle = objectStoreView.awaitAny();
         do {
             if (handle->resultPtr == nullptr) [[unlikely]] {
                 throw std::logic_error("Did not find item: " + std::to_string(handle->key));
             }
             handle->key = keyQueryOrder[handled];
             objectStoreView.enqueueQuery(handle);
             handle = objectStoreView.peekAny();
             handled++;
         } while (handle != nullptr);
         objectStoreView.submit();
         pachash::LOG("Querying", handled/32, numQueries/32);
     }
     auto queryEnd = std::chrono::high_resolution_clock::now();

     // Collect remaining in-flight queries
     for (size_t i = 0; i < depth; i++) {
         pachash::QueryHandle *handle = objectStoreView.awaitAny();
         if (handle->resultPtr == nullptr) {
             throw std::logic_error("Did not find item: " + std::to_string(handle->key));
         }
     }

     long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
     std::cout << "\r\033[KQuery benchmark completed."<<std::endl;
     std::cout << "RESULT"
               << " method=" << ObjectStore::name()
               << " queries=" << handled
               << " keys=" << numKeys
               << " milliseconds=" << timeMilliseconds
               << " kqueriesPerSecond=" << (double)handled/(double)timeMilliseconds
               << " internalSpace=" << objectStore.internalSpaceUsage()
               << " file=" << storeFile.substr(storeFile.find_last_of("/\\") + 1)
               << std::endl;
 }


template <size_t ...> struct IntList {};

template<template<size_t _> class ObjectStore>
void dispatchObjectStore(size_t param, IntList<>) {
    std::cerr<<"The parameter "<<param<<" for "<<typeid(ObjectStore<0>).name()<<" was not compiled into this binary."<<std::endl;
}
template <template<size_t _> class ObjectStore, size_t I, size_t ...ListRest>
void dispatchObjectStore(size_t param, IntList<I, ListRest...>) {
    if (I != param) {
        return dispatchObjectStore<ObjectStore, ListRest...>(param, IntList<ListRest...>());
    } else {
        performQueries<ObjectStore<I>>();
    }
}

template <uint16_t a>
using PaCHashWithUncompressedBitVectorIndex = pachash::PaCHashObjectStore<a, pachash::UncompressedBitVectorIndex>;

int main(int argc, char** argv) {
    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", storeFile, "Object store to query");
    cmd.add_bytes('n', "num_queries", numQueries, "Number of queries to benchmark");
    cmd.add_flag('c', "cached_io", useCachedIo, "Use cached instead of direct IO");
    cmd.add_size_t('a', "a", pachashParameterA, "Parameter for PaCHash index generation");
    cmd.add_string('t', "index_type", indexType, "Indexing method to use. Possible values: eliasFano, uncompressedBitVector");
    if (!cmd.process(argc, argv)) {
        return 1;
    }

    auto metadata = pachash::VariableSizeObjectStore::readMetadata(storeFile.c_str());
    if (metadata.type == pachash::VariableSizeObjectStore::StoreMetadata::TYPE_PACHASH) {
        std::cout<<"Reading keys"<<std::endl;
        pachash::LinearObjectReader<false> reader(storeFile.c_str(), useCachedIo ? 0 : O_DIRECT);
        while (!reader.hasEnded()) {
            keys.push_back(reader.currentKey);
            pachash::LOG("Reading keys", reader.currentBlock, reader.numBlocks);
            reader.next();
        }
        pachash::LOG(nullptr);
        std::cout<<"Querying PacHash store"<<std::endl;

        if (indexType == "eliasFano") {
            dispatchObjectStore<pachash::PaCHashObjectStore>(pachashParameterA, IntList<1, 2, 4, 8, 16, 32, 64, 128>());
        } else if (indexType == "uncompressedBitVector") {
            dispatchObjectStore<PaCHashWithUncompressedBitVectorIndex>(pachashParameterA, IntList<1, 2, 4, 8, 16, 32, 64, 128>());
        } else {
            cmd.print_usage();
            return 1;
        }
    } else {
        pachash::UringDoubleBufferBlockIterator iterator(storeFile.c_str(),
                 metadata.numBlocks, 128, useCachedIo ? 0 : O_DIRECT);
        std::cout<<"Reading keys"<<std::endl;
        for (size_t i = 0; i < metadata.numBlocks; i++) {
            pachash::VariableSizeObjectStore::BlockStorage storage(iterator.blockContent());
            for (size_t k = 0; k < storage.numObjects; k++) {
                if (i == 0 && k == 0) {
                    continue;
                }
                keys.push_back(storage.keys[k]);
            }
            if (i != metadata.numBlocks - 1) {
                iterator.next();
            }
            pachash::LOG("Reading keys", i, metadata.numBlocks);
        }
        pachash::LOG(nullptr);

        if (metadata.type == pachash::VariableSizeObjectStore::StoreMetadata::TYPE_CUCKOO) {
            std::cout<<"Querying Cuckoo store"<<std::endl;
            performQueries<pachash::ParallelCuckooObjectStore>();
        } else if (metadata.type == pachash::VariableSizeObjectStore::StoreMetadata::TYPE_SEPARATOR + separatorBits) {
            std::cout<<"Querying Separator store"<<std::endl;
            performQueries<pachash::SeparatorObjectStore<separatorBits>>();
        } else {
            std::cerr<<"Unknown object store type"<<std::endl;
            return 1;
        }
    }
    return 0;
}
