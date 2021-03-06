#include <PaCHashObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <tlx/cmdline_parser.hpp>
#include <MurmurHash64.h>
#include <Files.h>

struct GeneEntry {
    pachash::StoreConfig::key_t key;
    size_t length;
    char *beginOfValue;
};

/**
 * Advanced construction example. The input data is read from a fasta file, which has the following format:
 * >title comment
 * data with line breaks after 100 characters
 * >title comment
 * data with line breaks after 100 characters
 *
 * We assume here that the data is too much to load into an std::vector at once. So we need to re-construct
 * the values (eg. remove line breaks) dynamically while the data is written. We do that by providing a more complex
 * value extractor function that uses some temporary memory to write the data to. This is possible because the object
 * stores do not call the key extractor with other keys before the previous one is fully consumed.
 *
 * The performance depends on random disk read times because the key extractor
 * works on a memory-mapped file and is not called linearly.
 */
int main(int argc, char** argv) {
    std::string inputFile = "uniref50.fasta";
    std::string outputFile = "key_value_store.db";
    std::string type = "pachash";
    bool cachedIo = false;
    size_t dropLargeObjects = ~0ul;

    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", inputFile, "Tweet input file");
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
    size_t fileSize = util::filesize(fd);
    char *data = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));

    std::vector<GeneEntry> genes;
    GeneEntry currentEntry = {};
    char *pos = data;
    while (pos < data + fileSize) {
        if (*pos == '>') {
            if (currentEntry.beginOfValue != nullptr) { // Has already found content
                if (currentEntry.length <= dropLargeObjects) {
                    genes.push_back(currentEntry);
                }
                currentEntry = {};
                if (genes.size() % 12123 == 0) {
                    std::cout << "\r\033[KGenes read: " << genes.size() << std::flush;
                }
            }
            pos++;
            char *nameStartPosition = pos;
            while (*pos != ' ') {
                pos++;
            }
            currentEntry.key = util::MurmurHash64(nameStartPosition, pos - nameStartPosition);
            while (*pos != '\n') {
                pos++; // Skip to beginning of sequence
            }
            currentEntry.beginOfValue = pos + 1;
        } else if (*pos != '\n') {
            currentEntry.length++;
        }
        pos++;
    }
    std::cout << "\r\033[KGenes read: " << genes.size() << std::endl;

    auto hashFunction = [](const GeneEntry &x) -> pachash::StoreConfig::key_t {
        return x.key;
    };
    auto lengthEx = [](const GeneEntry &x) -> size_t {
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

    pachash::VariableSizeObjectStore *objectStore;
    if (type == "pachash") {
        auto pachashStore = new pachash::PaCHashObjectStore<8>(1.0, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        pachashStore->writeToFile(genes.begin(), genes.end(), hashFunction, lengthEx, valueEx);
        objectStore = pachashStore;
    } else if (type == "cuckoo") {
        auto cuckooStore = new pachash::ParallelCuckooObjectStore(0.95, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        cuckooStore->writeToFile(genes.begin(), genes.end(), hashFunction, lengthEx, valueEx);
        objectStore = cuckooStore;
    } else if (type == "separator") {
        auto separatorStore = new pachash::SeparatorObjectStore<6>(0.95, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        separatorStore->writeToFile(genes.begin(), genes.end(), hashFunction, lengthEx, valueEx);
        objectStore = separatorStore;
    } else {
        throw std::logic_error("Invalid value for command line argument 'type'.");
    }

    objectStore->buildIndex();
    objectStore->printSizeHistogram(genes.begin(), genes.end(), lengthEx);
    objectStore->printConstructionStats();
    delete objectStore;
    return 0;
}
