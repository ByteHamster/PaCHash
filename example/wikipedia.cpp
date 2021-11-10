#include <EliasFanoObjectStore.h>
#include <libxml/xmlreader.h>
#include <lz4hc.h>
#include <regex>
#include <tlx/cmdline_parser.hpp>

size_t maxArticleSize = 1024 * 1024;
std::vector<std::pair<std::string, std::string>> wikipediaPages;
std::string inputFile = "enwiki-20210720-pages-meta-current1.xml";
std::string storeFile = "wikipedia_store.txt";

void construct() {
    std::string name;
    std::string value;
    char* compressionTargetBuffer = new char[maxArticleSize];

    xmlTextReaderPtr reader;
    reader = xmlReaderForFile(inputFile.c_str(), nullptr, 0);
    if (reader == nullptr) {
        fprintf(stderr, "Unable to open %s\n", inputFile.c_str());
        exit(1);
    }
    int ret = 1;
    while (ret == 1) {
        ret = xmlTextReaderRead(reader);
        const unsigned char *tagName = xmlTextReaderConstName(reader);
        if (tagName == nullptr) {
            continue;
        }
        if (xmlTextReaderNodeType(reader) == XML_READER_TYPE_END_ELEMENT) {
            if (memcmp("page", tagName, 4) == 0) {
                assert(value.length() < StoreConfig::MAX_OBJECT_SIZE);
                wikipediaPages.emplace_back(name, value);
                if (wikipediaPages.size() % 177 == 0) {
                    std::cout<<"\r\033[KRead: "<<wikipediaPages.size()<<" ("<<name<<")"<<std::flush;
                }
                if (wikipediaPages.size() >= 5000) {
                    ret = 0;
                    break;
                }
            }
        } else if (xmlTextReaderNodeType(reader) == XML_READER_TYPE_ELEMENT) {
            if (memcmp("title", tagName, 5) == 0) {
                xmlTextReaderRead(reader);
                name = (char*) xmlTextReaderConstValue(reader);
            } else if (memcmp("text", tagName, 4) == 0) {
                xmlTextReaderRead(reader);
                char *pageContentUncompressed = (char*) xmlTextReaderConstValue(reader);
                const size_t pageContentSize = strlen(pageContentUncompressed);
                assert(pageContentSize < maxArticleSize);
                const int compressedSize = LZ4_compress_HC(
                        pageContentUncompressed, compressionTargetBuffer, pageContentSize, maxArticleSize, 9);
                assert(compressedSize > 0);
                value = std::string(compressionTargetBuffer, compressedSize);
            }
        }
    }
    xmlFreeTextReader(reader);
    if (ret != 0) {
        fprintf(stderr, "%s : failed to parse\n", inputFile.c_str());
    }
    xmlCleanupParser();
    delete[] compressionTargetBuffer;
    std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages"<<std::endl;

    EliasFanoObjectStore<8> eliasFanoStore(1.0, storeFile.c_str(), 0);
    eliasFanoStore.writeToFile(wikipediaPages);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printConstructionStats();
}

void interactive() {
    EliasFanoObjectStore<8> eliasFanoStore(1.0, storeFile.c_str(), 0);
    eliasFanoStore.reloadFromFile();
    ObjectStoreView<EliasFanoObjectStore<8>, PosixIO> objectStoreView(eliasFanoStore, 0, 1);
    VariableSizeObjectStore::QueryHandle queryHandle;
    queryHandle.buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[eliasFanoStore.requiredBufferPerQuery()];

    std::cout<<"\r\033[K"<<std::endl<<"Interactive mode!"<<std::endl;
    char *articleDecompressed = new char[maxArticleSize];
    std::string line;
    while (true) {
        std::cout<<"# Enter page title: "<<std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }
        size_t blockLoadsBefore = queryHandle.stats.blocksFetched;
        queryHandle.prepare(line);
        objectStoreView.submitQuery(&queryHandle);
        objectStoreView.awaitAny(); // Only one query, so this returns the same handle again
        std::cout<<std::endl<<"########################## "<<line<<" ##########################"<<std::endl;
        if (queryHandle.resultPtr == nullptr) {
            std::cout<<"Not found."<<std::endl;
        } else {
            const int decompressedSize = LZ4_decompress_safe(
                    queryHandle.resultPtr, articleDecompressed, queryHandle.length, maxArticleSize);
            assert(decompressedSize >= 0);
            std::string result(articleDecompressed, decompressedSize);
            result = std::regex_replace(result, std::regex("=+([^\n=]*)=+ *\n"), "\033[42m\033[30m\033[1m$1\033[0m\n");
            std::cout << result<< std::endl;
            std::cout << "# Blocks fetched: " << (queryHandle.stats.blocksFetched - blockLoadsBefore) << std::endl;
        }
    }

    delete[] queryHandle.buffer;
}

void benchmark() {
    size_t depth = 128;
    EliasFanoObjectStore<8> eliasFanoStore(1.0, storeFile.c_str(), 0);
    eliasFanoStore.reloadFromFile();
    ObjectStoreView<EliasFanoObjectStore<8>, UringIO> objectStoreView(eliasFanoStore, 0, depth);
    std::vector<VariableSizeObjectStore::QueryHandle> queryHandles(depth);
    for (auto &handle : queryHandles) {
        handle.buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[eliasFanoStore.requiredBufferPerQuery()];
    }

    char *articleDecompressed = new char[maxArticleSize];

    std::cout<<"Benchmarking query performance..."<<std::flush;
    auto queryStart = std::chrono::high_resolution_clock::now();
    size_t handled = 0;
    for (size_t i = 0; i < depth; i++) {
        queryHandles[i].prepare(wikipediaPages.at(rand() % wikipediaPages.size()).first);
        objectStoreView.submitSingleQuery(&queryHandles[i]);
        handled++;
    }
    objectStoreView.submit();
    while (handled < 50000) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        do {
            const int decompressedSize = LZ4_decompress_safe(handle->resultPtr, articleDecompressed, handle->length, maxArticleSize);
            assert(decompressedSize >= 0);
            handle->prepare(wikipediaPages.at(rand() % wikipediaPages.size()).first);
            objectStoreView.submitSingleQuery(handle);
            handle = objectStoreView.peekAny();
            handled++;
        } while (handle != nullptr);
        objectStoreView.submit();
    }
    for (size_t i = 0; i < depth; i++) {
        VariableSizeObjectStore::QueryHandle *handle = objectStoreView.awaitAny();
        const int decompressedSize = LZ4_decompress_safe(handle->resultPtr, articleDecompressed, handle->length, maxArticleSize);
        assert(decompressedSize >= 0);
        handled++;
    }

    auto queryEnd = std::chrono::high_resolution_clock::now();
    long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
    std::cout << "\rQuery benchmark: " << handled << " queries in " << timeMilliseconds << " ms, "
              <<(double)handled/(double)timeMilliseconds << " kQueries/s" << std::endl;

    for (auto &handle : queryHandles) {
        delete[] handle.buffer;
    }
}

int main(int argc, char** argv) {
    if (StoreConfig::MAX_OBJECT_SIZE < maxArticleSize) {
        std::cerr<<"Wikipedia articles are long. The library needs to be compiled with more bits for object lengths."<<std::endl;
        std::cerr<<"Also, using block sizes that are significantly smaller than the average object size is not efficient."<<std::endl;
        exit(1);
    }
    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", inputFile, "Wikipedia xml input file");
    cmd.add_string('o', "output_file", storeFile, "Object store output file");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    construct();
    benchmark();
    interactive();
}
