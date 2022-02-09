#include <PaCHashObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <tlx/cmdline_parser.hpp>
#include <lz4.h>
#include "ipsx.h"

struct WikipediaPage {
    pachash::StoreConfig::key_t key;
    size_t length;
    const char *value;
    size_t compressedLength;
    WikipediaPage(unsigned long key, size_t length, const char *value, size_t compressedLength)
        : key(key), length(length), value(value), compressedLength(compressedLength) {
    }
};

/**
 * Advanced construction example. Reads wikipedia pages from an xml file and remembers pointers to the values.
 * The articles are stored in LZ4 compressed form. Because of limited RAM, we cannot keep all compressed articles in RAM
 * at all times. We therefore compress each article twice: once when reading in order to determine the length.
 * The second compression happens when actually writing the article.
 * For this, we use a more complex value extractor function that we pass to the object store.
 */
int main(int argc, char** argv) {
    size_t compressionBufferSize = 10 * 1024 * 1024;
    char *compressionBuffer = new char[compressionBufferSize];
    std::string inputFile = "enwiki-20210720-pages-meta-current1.xml";
    std::string outputFile = "key_value_store.db";
    std::string type = "pachash";
    bool cachedIo = false;
    size_t dropLargeObjects = ~0ul;

    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", inputFile, "Wikipedia xml input file");
    cmd.add_string('o', "output_file", outputFile, "Object store file");
    cmd.add_string('t', "type", type, "Object store type to generate");
    cmd.add_bool('c', "cached_io", cachedIo, "Use cached instead of direct IO");
    cmd.add_bytes('d', "drop_large_objects", dropLargeObjects, "Ignore objects that are larger than the given size");
    if (!cmd.process(argc, argv)) {
        return 1;
    }

    int fd = open(inputFile.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::ios_base::failure("Unable to open " + inputFile + ": " + std::string(strerror(errno)));
    }
    size_t fileSize = pachash::filesize(fd);
    char *xmlData = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
    madvise(xmlData, fileSize, MADV_SEQUENTIAL | MADV_WILLNEED);

    ipsx xmlParser(xmlData, fileSize);
    std::vector<WikipediaPage> wikipediaPages;
    ipsx::Node element = {};
    while (!xmlParser.hasEnded()) {
        xmlParser.readElementStart("page");
        if (xmlParser.hasEnded()) {
            break;
        }
        xmlParser.readElementStart("title");
        ipsx::Node title = xmlParser.readTextContent();
        pachash::StoreConfig::key_t key = pachash::MurmurHash64(title.pointer, title.length);
        if (wikipediaPages.size() % 4323 == 0) {
            std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages ("
                    <<std::string(title.pointer, title.length)<<")"<<std::flush;
        }
        xmlParser.readElementStart("ns");
        element = xmlParser.readTextContent();
        if (*element.pointer != '0') {
            continue; // Only read articles, not drafts etc
        }
        xmlParser.readElementStart("text");
        element = xmlParser.readTextContent();
        // Compress to determine size. Because we do not have enough RAM to store the compressed data,
        // we need to compress it again when actually writing.
        size_t compressedLength = LZ4_compress_default(
                element.pointer, compressionBuffer, element.length, compressionBufferSize);
        assert(compressedLength > 0);
        if (compressedLength <= dropLargeObjects) {
            wikipediaPages.emplace_back(key, element.length, element.pointer, compressedLength);
        }
    }
    std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages"<<std::endl;

    auto hashFunction = [](const WikipediaPage &page) -> pachash::StoreConfig::key_t {
        return page.key;
    };
    auto lengthEx = [](const WikipediaPage &page) -> size_t {
        return page.compressedLength;
    };
    auto valueEx = [compressionBuffer, compressionBufferSize](const WikipediaPage &page) -> const char * {
        LZ4_compress_default(page.value, compressionBuffer, page.length, compressionBufferSize);
        return compressionBuffer;
    };

    pachash::VariableSizeObjectStore *objectStore;
    if (type == "pachash") {
        auto pachashStore = new pachash::PaCHashObjectStore<8>(1.0, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        pachashStore->writeToFile(wikipediaPages.begin(), wikipediaPages.end(), hashFunction, lengthEx, valueEx);
        objectStore = pachashStore;
    } else if (type == "cuckoo") {
        auto cuckooStore = new pachash::ParallelCuckooObjectStore(0.95, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        cuckooStore->writeToFile(wikipediaPages.begin(), wikipediaPages.end(), hashFunction, lengthEx, valueEx);
        objectStore = cuckooStore;
    } else if (type == "separator") {
        auto separatorStore = new pachash::SeparatorObjectStore<6>(0.95, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        separatorStore->writeToFile(wikipediaPages.begin(), wikipediaPages.end(), hashFunction, lengthEx, valueEx);
        objectStore = separatorStore;
    } else {
        throw std::logic_error("Invalid value for command line argument 'type'.");
    }
    objectStore->reloadFromFile();
    objectStore->printSizeHistogram(wikipediaPages.begin(), wikipediaPages.end(), lengthEx);
    objectStore->printConstructionStats();
    delete objectStore;
    delete[] compressionBuffer;
    return 0;
}
