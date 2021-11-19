#include <EliasFanoObjectStore.h>
#include <libxml/xmlreader.h>
#include <lz4hc.h>
#include <tlx/cmdline_parser.hpp>

/**
 * Reads wikipedia pages from an xml file and writes them to a std::vector.
 * That vector is then directly passed to the object store.
 */
int main(int argc, char** argv) {
    std::string inputFile = "enwiki-20210720-pages-meta-current1.xml";
    std::string storeFile = "key_value_store.db";

    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", inputFile, "Wikipedia xml input file");
    cmd.add_string('o', "output_file", storeFile, "Object store file");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    std::string name;
    std::string value;
    std::vector<std::pair<std::string, std::string>> wikipediaPages;
    char compressionTargetBuffer[1024 * 1024];
    xmlTextReaderPtr reader;
    reader = xmlReaderForFile(inputFile.c_str(), nullptr, 0);
    if (reader == nullptr) {
        throw std::ios_base::failure("Unable to open " + inputFile);
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
                assert(pageContentSize < sizeof(compressionTargetBuffer)/sizeof(char));
                const int compressedSize = LZ4_compress_HC(pageContentUncompressed, compressionTargetBuffer,
                       pageContentSize, sizeof(compressionTargetBuffer)/sizeof(char), 9);
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
    std::cout<<"\r\033[KRead "<<wikipediaPages.size()<<" pages"<<std::endl;

    EliasFanoObjectStore<8> eliasFanoStore(1.0, storeFile.c_str(), O_DIRECT);
    eliasFanoStore.writeToFile(wikipediaPages);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printSizeHistogram(wikipediaPages);
    eliasFanoStore.printConstructionStats();
    return 0;
}
