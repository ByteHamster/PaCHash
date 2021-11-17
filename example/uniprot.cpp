#include <EliasFanoObjectStore.h>

struct GeneEntry {
    StoreConfig::key_t key;
    size_t length;
    char *beginOfValue;
};

int main(int argc, char** argv) {
    std::string filename = "uniref50.fasta";
    int fd = open(filename.c_str(), O_RDONLY);
    assert(fd >= 0);
    size_t fileSize = filesize(fd);
    char *data = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));

    auto hashFunction = [](const GeneEntry &x) -> StoreConfig::key_t {
        return x.key;
    };
    auto lengthEx = [](const GeneEntry &x) -> StoreConfig::length_t {
        return x.length;
    };
    char *reconstructionBuffer = new char[50000];
    auto valueEx = [reconstructionBuffer](const GeneEntry &x) -> const char * {
        char *pos = x.beginOfValue;
        size_t length = 0;
        while (length < x.length) {
            if (*pos != '\n') {
                reconstructionBuffer[length] = *pos;
                length++;
            }
            pos++;
        }
        return reconstructionBuffer;
    };

    std::vector<GeneEntry> genes;
    GeneEntry currentEntry = {};
    char *pos = data;
    while (pos < data + fileSize) {
        if (*pos == '>') {
            if (currentEntry.beginOfValue != nullptr) { // Has already found content
                genes.push_back(currentEntry);
                currentEntry = {};
                if (genes.size() % 12123 == 0) {
                    std::cout<<"\r\033[KGenes read: "<<genes.size()<<std::flush;
                }
            }
            pos++;
            char *nameStartPosition = pos;
            while (*pos != ' ') {
                pos++;
            }
            currentEntry.key = MurmurHash64(nameStartPosition, pos-nameStartPosition);
            while (*pos != '\n') {
                pos++; // Skip to beginning of sequence
            }
            currentEntry.beginOfValue = pos + 1;
        } else if (*pos != '\n') {
            currentEntry.length++;
        }
        pos++;
    }

    std::cout<<"\r\033[KGenes read: "<<genes.size()<<std::endl;
    EliasFanoObjectStore<8> eliasFanoStore(1.0, "/dev/nvme0n1", O_DIRECT);
    eliasFanoStore.writeToFile(genes.begin(), genes.end(), hashFunction, lengthEx, valueEx);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printSizeHistogram(genes.begin(), genes.end(), lengthEx);
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
        queryHandles[i].key = genes.at(rand() % genes.size()).key;
        objectStoreView.submitSingleQuery(&queryHandles[i]);
        handled++;
    }
    objectStoreView.submit();
    while (handled < numQueries) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        do {
            assert(handle->resultPtr != nullptr);
            handle->key = genes.at(rand() % genes.size()).key;
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
