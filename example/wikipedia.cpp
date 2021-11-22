#include <EliasFanoObjectStore.h>
#include <tlx/cmdline_parser.hpp>
#include "ipsx.h"

struct WikipediaPage {
    StoreConfig::key_t key;
    size_t length;
    const char *value;
    WikipediaPage(unsigned long key, size_t length, const char *value) : key(key), length(length), value(value) {}
};

/**
 * Intermediate construction example. Reads wikipedia pages from an xml file and remembers pointers to the values.
 * These pointers are then passed to the object store. Note that this example does not need to reserve
 * RAM for the values - it just points to locations in a memory mapped file.
 */
int main(int argc, char** argv) {
    std::string inputFile = "enwiki-20210720-pages-meta-current1.xml";
    std::string storeFile = "key_value_store.db";

    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", inputFile, "Wikipedia xml input file");
    cmd.add_string('o', "output_file", storeFile, "Object store file");

    if (!cmd.process(argc, argv)) {
        return 1;
    } else if (sizeof(StoreConfig::length_t) <= sizeof(uint16_t)) {
        std::cerr<<"Compiled with a length type that is too short for storing Wikipedia articles. See StoreConfig::length_t."<<std::endl;
        return 1;
    }

    int fd = open(inputFile.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::ios_base::failure("Unable to open " + inputFile + ": " + std::string(strerror(errno)));
    }
    size_t fileSize = filesize(fd);
    char *xmlData = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
    madvise(xmlData, fileSize, MADV_SEQUENTIAL | MADV_WILLNEED);

    ipsx xmlParser(xmlData, fileSize);
    VariableSizeObjectStore::LOG("Parsing articles");
    std::vector<WikipediaPage> wikipediaPages;
    ipsx::Node element = {};
    while (!xmlParser.hasEnded()) {
        do {
            element = xmlParser.readElementStart();
        } while (memcmp(element.pointer, "page", element.length) != 0);
        if (xmlParser.hasEnded()) {
            break;
        }
        do {
            element = xmlParser.readElementStart();
        } while (memcmp(element.pointer, "title", element.length) != 0);
        element = xmlParser.readTextContent();
        StoreConfig::key_t key = MurmurHash64(element.pointer, element.length);
        if (wikipediaPages.size() % 4323 == 0) {
            std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages ("<<std::string(element.pointer, element.length)<<")"<<std::flush;
        }
        do {
            element = xmlParser.readElementStart();
        } while (memcmp(element.pointer, "text", element.length) != 0);
        element = xmlParser.readTextContent();
        wikipediaPages.emplace_back(key, element.length, element.pointer);
    }
    std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages"<<std::endl;

    auto hashFunction = [](const WikipediaPage &page) -> StoreConfig::key_t {
        return page.key;
    };
    auto lengthEx = [](const WikipediaPage &page) -> StoreConfig::length_t {
        return page.length;
    };
    auto valueEx = [](const WikipediaPage &page) -> const char * {
        return page.value;
    };

    EliasFanoObjectStore<8> eliasFanoStore(1.0, storeFile.c_str(), O_DIRECT);
    eliasFanoStore.writeToFile(wikipediaPages.begin(), wikipediaPages.end(), hashFunction, lengthEx, valueEx);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printSizeHistogram(wikipediaPages.begin(), wikipediaPages.end(), lengthEx);
    eliasFanoStore.printConstructionStats();
    return 0;
}
