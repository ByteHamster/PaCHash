#include <EliasFanoObjectStore.h>
#include <LinearObjectReader.h>
#include <tlx/cmdline_parser.hpp>

/**
 * This more complex example submits multiple queries to be kept in-flight at the same time.
 * If a query is completed, it prepares a new one. If there is no query to process, it submits the prepared ones.
 * The keys for the queries are read from the input file when starting.
 */
int main(int argc, char** argv) {
    std::string storeFile = "key_value_store.db";
    size_t numQueries = 1000;

    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", storeFile, "Object store to query");
    cmd.add_bytes('n', "num_queries", numQueries, "Number of queries to benchmark");
    if (!cmd.process(argc, argv)) {
        return 1;
    }

    std::vector<StoreConfig::key_t> keys;
    LinearObjectReader reader(storeFile.c_str(), O_DIRECT);
    while (!reader.hasEnded()) {
        keys.push_back(reader.currentKey());
        VariableSizeObjectStore::LOG("Reading keys", reader.currentBlock, reader.numBlocks);
        reader.next();
    }
    size_t numKeys = keys.size();

    size_t depth = 128;
    EliasFanoObjectStore<8> eliasFanoStore(1.0, storeFile.c_str(), O_DIRECT);
    eliasFanoStore.reloadFromFile();

    ObjectStoreView<EliasFanoObjectStore<8>, UringIO> objectStoreView(eliasFanoStore, O_DIRECT, depth);
    std::vector<VariableSizeObjectStore::QueryHandle> queryHandles(depth);
    for (auto &handle : queryHandles) {
        handle.buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[eliasFanoStore.requiredBufferPerQuery()];
    }

    std::cout<<"Benchmarking query performance..."<<std::flush;
    auto queryStart = std::chrono::high_resolution_clock::now();
    size_t handled = 0;

    // Fill in-flight queue
    for (size_t i = 0; i < depth; i++) {
        queryHandles[i].key = keys[rand() % numKeys];
        objectStoreView.submitSingleQuery(&queryHandles[i]);
        handled++;
    }
    objectStoreView.submit();

    // Submit new queries as old ones complete
    while (handled < numQueries) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        do {
            if (handle->resultPtr == nullptr) {
                throw std::logic_error("Did not find item");
            }
            handle->key = keys[rand() % numKeys];
            objectStoreView.submitSingleQuery(handle);
            handle = objectStoreView.peekAny();
            handled++;
        } while (handle != nullptr);
        objectStoreView.submit();
    }

    // Collect remaining in-flight queries
    for (size_t i = 0; i < depth; i++) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        if (handle->resultPtr == nullptr) {
            throw std::logic_error("Did not find item");
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

    for (auto &handle : queryHandles) {
        delete[] handle.buffer;
    }
    return 0;
}
