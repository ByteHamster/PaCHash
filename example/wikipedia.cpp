#include <EliasFanoObjectStore.h>
#include <libxml/xmlreader.h>
#include <gzip/compress.hpp>
#include <gzip/decompress.hpp>

struct Item {
    char *pointer;
    StoreConfig::length_t length;
};

class MapObjectProvider : public ObjectProvider {
    public:
        std::unordered_map<uint64_t, Item> items;
        [[nodiscard]] StoreConfig::length_t getLength(StoreConfig::key_t key) final {
            return items.at(key).length;
        }

        [[nodiscard]] const char *getValue(StoreConfig::key_t key) final {
            return items.at(key).pointer;
        }
};

int main() {
    std::string name;
    std::string value;
    size_t pagesRead = 0;
    std::vector<uint64_t> keys;
    std::vector<std::string> values;
    MapObjectProvider objectProvider;

    xmlTextReaderPtr reader;
    const char *filename = "enwiki-20210720-pages-meta-current1.xml";
    reader = xmlReaderForFile(filename, nullptr, 0);
    if (reader == nullptr) {
        fprintf(stderr, "Unable to open %s\n", filename);
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
                uint64_t key = MurmurHash64(name.data(), name.length());
                keys.emplace_back(key);
                values.emplace_back(value);
                objectProvider.items.insert(std::make_pair(key, Item {values.back().data(), static_cast<StoreConfig::length_t>(value.length())}));
                pagesRead++;
                if (pagesRead % 500 == 0) {
                    std::cout<<"\r\033[KRead: "<<pagesRead<<" ("<<name<<")"<<std::flush;
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
        fprintf(stderr, "%s : failed to parse\n", filename);
    }
    xmlCleanupParser();
    std::cout<<"\r\033[KRead "<<pagesRead<<" pages"<<std::endl;

    EliasFanoObjectStore<8> eliasFanoStore(1.0, "key_value_store.txt", 0);
    eliasFanoStore.writeToFile(keys, objectProvider);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printConstructionStats();

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
        queryHandle.key = MurmurHash64(line.data(), line.length());
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
