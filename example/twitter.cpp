#include <EliasFanoObjectStore.h>

int main(int argc, char** argv) {
    std::ifstream input("twitter-stream-2021-08-01.txt");
    std::string line;
    std::vector<std::pair<std::string, std::string>> tweets;
    while (std::getline(input,line)) {
        size_t spacePosition = line.find_first_of(' ');
        tweets.emplace_back(line.substr(0, spacePosition), line.substr(spacePosition+1));
        if (tweets.size() % 12123 == 0) {
            std::cout<<"\r\033[KTweets read: "<<tweets.size()<<std::flush;
        }
    }

    std::cout<<"\r\033[KTweets read: "<<tweets.size()<<std::endl;
    VariableSizeObjectStore::printSizeHistogram(tweets);
    EliasFanoObjectStore<8> eliasFanoStore(1.0, "/dev/nvme0n1", O_DIRECT);
    eliasFanoStore.writeToFile(tweets);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printConstructionStats();

    size_t depth = 128;
    size_t numQueries = 5000000;
    ObjectStoreView<EliasFanoObjectStore<8>, UringIO> objectStoreView(eliasFanoStore, O_DIRECT, depth);
    std::vector<VariableSizeObjectStore::QueryHandle> queryHandles;
    queryHandles.resize(depth);
    for (auto &handle : queryHandles) {
        handle.buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[eliasFanoStore.requiredBufferPerQuery()];
        assert(handle.buffer != nullptr);
    }

    std::cout<<"Benchmarking query performance..."<<std::flush;
    auto queryStart = std::chrono::high_resolution_clock::now();
    size_t handled = 0;
    for (size_t i = 0; i < depth; i++) {
        queryHandles[i].prepare(tweets.at(rand() % tweets.size()).first);
        objectStoreView.submitSingleQuery(&queryHandles[i]);
        handled++;
    }
    objectStoreView.submit();
    while (handled < numQueries) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        do {
            assert(handle->resultPtr != nullptr);
            handle->prepare(tweets.at(rand() % tweets.size()).first);
            objectStoreView.submitSingleQuery(handle);
            handle = objectStoreView.peekAny();
            handled++;
        } while (handle != nullptr);
        objectStoreView.submit();
    }
    for (size_t i = 0; i < depth; i++) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        assert(handle->resultPtr != nullptr);
        handled++;
    }

    auto queryEnd = std::chrono::high_resolution_clock::now();
    long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
    std::cout << "\r\033[KQuery benchmark completed."<<std::endl;
    std::cout << "RESULT"
              << " n=" << handled
              << " milliseconds=" << timeMilliseconds
              << " kqueriesPerSecond=" << (double)handled/(double)timeMilliseconds
              << std::endl;

    for (auto &handle : queryHandles) {
        delete[] handle.buffer;
    }
}
