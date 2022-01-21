#include <PaCHashObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <SeparatorObjectStore.h>
#include <LinearObjectReader.h>
#include <tlx/cmdline_parser.hpp>

std::string storeFile = "key_value_store.db";
size_t numQueries = 1000;
bool useCachedIo = false;

/**
 * This more complex example submits multiple queries to be kept in-flight at the same time.
 * If a query is completed, it prepares a new one. If there is no query to process, it submits the prepared ones.
 * The keys for the queries are read from the input file when starting.
 */
 template <typename ObjectStore>
 void performQueries(std::vector<pachash::StoreConfig::key_t> &keys) {
    size_t numKeys = keys.size();
    size_t depth = 128;
    ObjectStore objectStore(1.0, storeFile.c_str(), useCachedIo ? 0 : O_DIRECT);
    objectStore.reloadFromFile();

    pachash::ObjectStoreView<ObjectStore, pachash::UringIO> objectStoreView(objectStore, useCachedIo ? 0 : O_DIRECT, depth);
    std::vector<pachash::QueryHandle> queryHandles;
    queryHandles.reserve(depth);
    for (size_t i = 0; i < depth; i++) {
        queryHandles.emplace_back(objectStore);
    }

    auto queryStart = std::chrono::high_resolution_clock::now();
    size_t handled = 0;

    // Fill in-flight queue
    for (size_t i = 0; i < depth; i++) {
        queryHandles[i].key = keys[rand() % numKeys];
        objectStoreView.enqueueQuery(&queryHandles[i]);
        handled++;
    }
    objectStoreView.submit();

    // Submit new queries as old ones complete
    while (handled < numQueries) {
        pachash::QueryHandle *handle = objectStoreView.awaitAny();
        do {
            if (handle->resultPtr == nullptr) {
                throw std::logic_error("Did not find item: " + std::to_string(handle->key));
            }
            handle->key = keys[rand() % numKeys];
            objectStoreView.enqueueQuery(handle);
            handle = objectStoreView.peekAny();
            handled++;
        } while (handle != nullptr);
        objectStoreView.submit();
        objectStore.LOG("Querying", handled/32, numQueries/32);
    }

    // Collect remaining in-flight queries
    for (size_t i = 0; i < depth; i++) {
        pachash::QueryHandle *handle = objectStoreView.awaitAny();
        if (handle->resultPtr == nullptr) {
            throw std::logic_error("Did not find item: " + std::to_string(handle->key));
        }
        handled++;
    }

    auto queryEnd = std::chrono::high_resolution_clock::now();
    long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
    std::cout << "\r\033[KQuery benchmark completed."<<std::endl;
    std::cout << "RESULT"
              << " queries=" << handled
              << " keys=" << numKeys
              << " milliseconds=" << timeMilliseconds
              << " kqueriesPerSecond=" << (double)handled/(double)timeMilliseconds
              << std::endl;
 }

int main(int argc, char** argv) {
    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", storeFile, "Object store to query");
    cmd.add_bytes('n', "num_queries", numQueries, "Number of queries to benchmark");
    cmd.add_flag('c', "cached_io", useCachedIo, "Use cached instead of direct IO");
    if (!cmd.process(argc, argv)) {
        return 1;
    }

    constexpr uint8_t pachashParameterA = 8;
    constexpr uint8_t separatorBits = 6;

    auto metadata = pachash::VariableSizeObjectStore::readMetadata(storeFile.c_str());
    if (metadata.type == pachash::VariableSizeObjectStore::StoreMetadata::TYPE_PACHASH + pachashParameterA) {
        std::vector<pachash::StoreConfig::key_t> keys;
        pachash::LinearObjectReader<false> reader(storeFile.c_str(), useCachedIo ? 0 : O_DIRECT);
        while (!reader.hasEnded()) {
            keys.push_back(reader.currentKey);
            pachash::VariableSizeObjectStore::LOG("Reading keys", reader.currentBlock, reader.numBlocks);
            reader.next();
        }
        pachash::VariableSizeObjectStore::LOG(nullptr);
        std::cout<<"Querying PacHash store"<<std::endl;
        performQueries<pachash::PaCHashObjectStore<pachashParameterA>>(keys);
    } else {
        std::vector<pachash::StoreConfig::key_t> keys;
        pachash::UringDoubleBufferBlockIterator iterator(storeFile.c_str(), metadata.numBlocks, 128, useCachedIo ? 0 : O_DIRECT);
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
            pachash::VariableSizeObjectStore::LOG("Reading keys", i, metadata.numBlocks);
        }
        pachash::VariableSizeObjectStore::LOG(nullptr);

        if (metadata.type == pachash::VariableSizeObjectStore::StoreMetadata::TYPE_CUCKOO) {
            std::cout<<"Querying Cuckoo store"<<std::endl;
            performQueries<pachash::ParallelCuckooObjectStore>(keys);
        } else if (metadata.type == pachash::VariableSizeObjectStore::StoreMetadata::TYPE_SEPARATOR + separatorBits) {
            std::cout<<"Querying Separator store"<<std::endl;
            performQueries<pachash::SeparatorObjectStore<separatorBits>>(keys);
        } else {
            std::cerr<<"Unknown object store type"<<std::endl;
            return 1;
        }
    }
    return 0;
}
