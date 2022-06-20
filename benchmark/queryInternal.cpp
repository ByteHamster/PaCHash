#include <PaCHashObjectStore.h>
#include <LinearObjectReader.h>
#include <tlx/cmdline_parser.hpp>
#include <Util.h>

#define DO_NOT_OPTIMIZE(value) asm volatile ("" : : "r,m"(value) : "memory")

std::string storeFile = "key_value_store.db";
size_t numQueries = 1000;
std::vector<pachash::StoreConfig::key_t> keys;
size_t pachashParameterA = 8;
std::string indexType = "eliasFano";

 template <typename ObjectStore>
 void performQueries() {
     size_t numKeys = keys.size();
     size_t depth = 128;
     ObjectStore objectStore(1.0, storeFile.c_str(), 0);
     objectStore.buildIndex();

     pachash::XorShift64 prng(time(nullptr));
     // Accessed linearly at query time, while `keys` array would be accessed randomly
     std::vector<pachash::StoreConfig::key_t> keyQueryOrder;
     keyQueryOrder.reserve(numQueries + depth);
     for (size_t i = 0; i < numQueries + depth; i++) {
         keyQueryOrder.push_back(keys.at(prng(numKeys)));
         pachash::LOG("Preparing list of keys to query", i, numQueries);
     }
     std::tuple<size_t, size_t> accessDetails;

     auto queryStart = std::chrono::high_resolution_clock::now();
     // Submit new queries as old ones complete
     for (size_t i = 0; i < numQueries; i++) {
         objectStore.index->locate(objectStore.key2bin(keyQueryOrder[i]), accessDetails);
         DO_NOT_OPTIMIZE(accessDetails);
     }
     auto queryEnd = std::chrono::high_resolution_clock::now();
     long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
     std::cout << "\r\033[KQuery benchmark completed."<<std::endl;
     std::cout << "RESULT"
               << " method=" << ObjectStore::name()
               << " queries=" << numQueries
               << " keys=" << numKeys
               << " milliseconds=" << timeMilliseconds
               << " kqueriesPerSecond=" << (double)numQueries/(double)timeMilliseconds
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

template <uint16_t a>
using PaCHashWithCompressedBitVectorIndex = pachash::PaCHashObjectStore<a, pachash::CompressedBitVectorIndex>;

template <uint16_t a>
using PaCHashWithLaVectorIndex = pachash::PaCHashObjectStore<a, pachash::LaVectorIndex>;

int main(int argc, char** argv) {
    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", storeFile, "Object store to query");
    cmd.add_bytes('n', "num_queries", numQueries, "Number of queries to benchmark");
    cmd.add_size_t('a', "a", pachashParameterA, "Parameter for PaCHash index generation");
    cmd.add_string('t', "index_type", indexType, "Indexing method to use. Possible values: eliasFano, uncompressedBitVector, compressedBitVector, laVector");
    if (!cmd.process(argc, argv)) {
        return 1;
    }

    auto metadata = pachash::VariableSizeObjectStore::readMetadata(storeFile.c_str());
    if (metadata.type != pachash::VariableSizeObjectStore::StoreMetadata::TYPE_PACHASH) {
        return 1;
    }
    std::cout<<"Reading keys"<<std::endl;
    pachash::LinearObjectReader<false> reader(storeFile.c_str(), 0);
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
    } else if (indexType == "compressedBitVector") {
        dispatchObjectStore<PaCHashWithCompressedBitVectorIndex>(pachashParameterA, IntList<1, 2, 4, 8, 16, 32, 64, 128>());
    } else if (indexType == "laVector") {
        dispatchObjectStore<PaCHashWithLaVectorIndex>(pachashParameterA, IntList<1, 2, 4, 8, 16, 32, 64, 128>());
    } else {
        cmd.print_usage();
        return 1;
    }
    return 0;
}
