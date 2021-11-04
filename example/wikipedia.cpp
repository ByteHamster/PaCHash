#include <EliasFanoObjectStore.h>
#include <libxml/xmlreader.h>
#include <gzip/compress.hpp>
#include <gzip/decompress.hpp>

void construct(const char *inputFile, const char *outputFile) {
    std::string name;
    std::string value;
    std::vector<std::pair<std::string, std::string>> wikipediaPages;

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
                if (wikipediaPages.size() % 277 == 0) {
                    std::cout<<"\r\033[KRead: "<<wikipediaPages.size()<<" ("<<name<<")"<<std::flush;
                }
            }
        } else if (xmlTextReaderNodeType(reader) == XML_READER_TYPE_ELEMENT) {
            if (memcmp("title", tagName, 5) == 0) {
                xmlTextReaderRead(reader);
                name = (char*) xmlTextReaderConstValue(reader);
            } else if (memcmp("text", tagName, 4) == 0) {
                xmlTextReaderRead(reader);
                value = (char*) xmlTextReaderConstValue(reader);
                value = gzip::compress(value.data(), value.length());
            }
        }
    }
    xmlFreeTextReader(reader);
    if (ret != 0) {
        fprintf(stderr, "%s : failed to parse\n", inputFile);
    }
    xmlCleanupParser();
    std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages"<<std::endl;

    EliasFanoObjectStore<8> eliasFanoStore(1.0, outputFile, 0);
    eliasFanoStore.writeToFile(wikipediaPages);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printConstructionStats();
}

int main() {
    construct("enwiki-20210720-pages-meta-current1.xml", "wikipedia_store.txt");

    EliasFanoObjectStore<8> eliasFanoStore(1.0, "wikipedia_store.txt", 0);
    eliasFanoStore.reloadFromFile();
    ObjectStoreView<EliasFanoObjectStore<8>, PosixIO> objectStoreView(eliasFanoStore, 0, 1);
    VariableSizeObjectStore::QueryHandle queryHandle;
    queryHandle.buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[eliasFanoStore.requiredBufferPerQuery()];

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
            std::cout << gzip::decompress(queryHandle.resultPtr, queryHandle.length) << std::endl;
            std::cout << "# Blocks fetched: " << (queryHandle.stats.blocksFetched - blockLoadsBefore) << std::endl;
        }
    }

    delete[] queryHandle.buffer;
}
