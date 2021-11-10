#include <EliasFanoObjectStore.h>
#include <libxml/xmlreader.h>
#include <lz4hc.h>
#include <regex>

void construct(const char *inputFile, const char *outputFile) {
    std::string name;
    std::string value;
    std::vector<std::pair<std::string, std::string>> wikipediaPages;
    size_t compressionMaxSize = 1*1024*1024;
    char* compressionTargetBuffer = new char[compressionMaxSize];

    xmlTextReaderPtr reader;
    reader = xmlReaderForFile(inputFile, nullptr, 0);
    if (reader == nullptr) {
        fprintf(stderr, "Unable to open %s\n", inputFile);
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
            }
        } else if (xmlTextReaderNodeType(reader) == XML_READER_TYPE_ELEMENT) {
            if (memcmp("title", tagName, 5) == 0) {
                xmlTextReaderRead(reader);
                name = (char*) xmlTextReaderConstValue(reader);
            } else if (memcmp("text", tagName, 4) == 0) {
                xmlTextReaderRead(reader);
                char *pageContentUncompressed = (char*) xmlTextReaderConstValue(reader);
                const size_t pageContentSize = strlen(pageContentUncompressed);
                const int compressedSize = LZ4_compress_HC(
                        pageContentUncompressed, compressionTargetBuffer, pageContentSize, compressionMaxSize, 9);
                assert(compressedSize > 0);
                value = std::string(compressionTargetBuffer, compressedSize);
            }
        }
    }
    xmlFreeTextReader(reader);
    if (ret != 0) {
        fprintf(stderr, "%s : failed to parse\n", inputFile);
    }
    xmlCleanupParser();
    delete[] compressionTargetBuffer;
    std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages"<<std::endl;

    EliasFanoObjectStore<8> eliasFanoStore(1.0, outputFile, 0);
    eliasFanoStore.writeToFile(wikipediaPages);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printConstructionStats();
}

int main() {
    size_t maxArticleSize = 500 * 1024;
    if (StoreConfig::MAX_OBJECT_SIZE < maxArticleSize) {
        std::cerr<<"Wikipedia articles are long. The library needs to be compiled with more bits for object lengths."<<std::endl;
        std::cerr<<"Also, using block sizes that are significantly smaller than the average object size is not efficient."<<std::endl;
        exit(1);
    }
    construct("enwiki-20210720-pages-meta-current1.xml", "wikipedia_store.txt");

    EliasFanoObjectStore<8> eliasFanoStore(1.0, "wikipedia_store.txt", 0);
    eliasFanoStore.reloadFromFile();
    ObjectStoreView<EliasFanoObjectStore<8>, PosixIO> objectStoreView(eliasFanoStore, 0, 1);
    VariableSizeObjectStore::QueryHandle queryHandle;
    queryHandle.buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[eliasFanoStore.requiredBufferPerQuery()];

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
