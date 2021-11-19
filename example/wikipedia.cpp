#include <EliasFanoObjectStore.h>
#include <tlx/cmdline_parser.hpp>
#include <rapidxml.hpp>

struct WikipediaPage {
    StoreConfig::key_t key;
    size_t length;
    char *value;
    WikipediaPage(unsigned long key, size_t length, char *value) : key(key), length(length), value(value) {}
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
    char *xmlData = static_cast<char *>(mmap(nullptr, filesize(fd), PROT_READ, MAP_PRIVATE, fd, 0));
    VariableSizeObjectStore::LOG("Parsing XML");
    rapidxml::xml_document<> doc;
    doc.parse<rapidxml::parse_non_destructive>(xmlData);

    VariableSizeObjectStore::LOG("Collecting XML content");
    std::vector<WikipediaPage> wikipediaPages;
    rapidxml::xml_node<> *page = doc.first_node("mediawiki")->first_node("page");
    while (page != nullptr) {
        rapidxml::xml_node<> *title = page->first_node("title");
        StoreConfig::key_t key = MurmurHash64(title->value(), title->value_size());
        rapidxml::xml_node<> *text = page->first_node("revision")->first_node("text");
        wikipediaPages.emplace_back(key, text->value_size(), text->value());
        page = page->next_sibling("page");
    }

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
