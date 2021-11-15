#include <EliasFanoObjectStore.h>

int main(int argc, char** argv) {
    std::ifstream input("uniprot-filtered-reviewed-yes.fasta");
    if (errno != 0) {
        std::cerr<<strerror(errno)<<std::endl;
    }
    std::string name;
    std::string content;
    std::string line;
    std::vector<std::pair<std::string, std::string>> genes;
    while (std::getline(input,line)) {
        if (line.starts_with('>')) {
            if (!name.empty()) {
                genes.emplace_back(name, content);
                content = "";
                if (genes.size() % 12123 == 0) {
                    std::cout<<"\r\033[KGenes read: "<<genes.size()<<std::flush;
                }
            }
            size_t pipePosition1 = line.find_first_of('|') + 1;
            size_t pipePosition2 = line.find_first_of('|', pipePosition1);
            name = line.substr(pipePosition1, pipePosition2 - pipePosition1);
        } else {
            content += line;
        }
    }

    std::cout<<"\r\033[KGenes read: "<<genes.size()<<std::endl;
    VariableSizeObjectStore::printSizeHistogram(genes);
    EliasFanoObjectStore<8> eliasFanoStore(1.0, "/dev/nvme0n1", O_DIRECT);
    eliasFanoStore.writeToFile(genes);
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
        queryHandles[i].prepare(genes.at(rand() % genes.size()).first);
        objectStoreView.submitSingleQuery(&queryHandles[i]);
        handled++;
    }
    objectStoreView.submit();
    while (handled < numQueries) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        do {
            assert(handle->resultPtr != nullptr);
            handle->prepare(genes.at(rand() % genes.size()).first);
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
