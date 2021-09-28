#include <chrono>
#include <thread>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>
#include <tlx/cmdline_parser.hpp>

class LinearObjectReader {
    private:
        size_t currentBlock = -1;
        size_t currentElement = -1;
        char *file;
        VariableSizeObjectStore::BlockStorage *block = nullptr;
        size_t numBlocks;
        int fd;
    public:
        explicit LinearObjectReader(const char *filename) {
            numBlocks = EliasFanoObjectStore<8>::readSpecialObject0(filename);
            fd = open(filename, O_RDONLY);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }

            size_t fileSize = numBlocks * PageConfig::PAGE_SIZE;
            file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
            assert(file != MAP_FAILED);

            next(); // Initialize
            next(); // Skip pseudo object 0
        }

        ~LinearObjectReader() {
            delete block;
            block = nullptr;
        }

        bool hasMore() {
            assert(block != nullptr);
            return currentBlock < numBlocks - 1 || currentElement < block->numObjects - 1;
        }

        void next() {
            if (block == nullptr || currentElement == block->numObjects - 1) {
                currentBlock++;
                currentElement = 0;
                delete block;
                block = new VariableSizeObjectStore::BlockStorage(file + currentBlock * PageConfig::PAGE_SIZE);
                block->calculateObjectPositions();
            } else {
                currentElement++;
            }
        }

        VariableSizeObjectStore::Item current() {
            return VariableSizeObjectStore::Item {
                block->keys[currentElement],
                block->lengths[currentElement],
                reinterpret_cast<size_t>(block->objects[currentElement])
            };
        }
};

class SetObjectProvider : public ObjectProvider {
    public:
        std::unordered_map<uint64_t, uint16_t> lengths;
        std::unordered_map<uint64_t, char*> pointers;

        [[nodiscard]] size_t getLength(uint64_t key) final {
            return lengths.at(key);
        }

        [[nodiscard]] const char *getValue(uint64_t key) final {
            return pointers.at(key);
        }
};

int main(int argc, char** argv) {
    std::vector<std::string> inputFiles;
    std::string outputFile;

    tlx::CmdlineParser cmd;
    cmd.add_stringlist('i', "input_file", inputFiles, "Input file that should be merged. Can be specified multiple times");
    cmd.add_string('o', "output_file", outputFile, "File to write the merged data structure to");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    if (inputFiles.empty() || outputFile.empty()) {
        std::cerr<<"Need input and output files"<<std::endl;
        cmd.print_usage();
        return 1;
    }

    std::vector<LinearObjectReader> readers;
    readers.reserve(inputFiles.size());
    for (const std::string& inputFile : inputFiles) {
        readers.emplace_back(inputFile.c_str());
    }

    VariableSizeObjectStore::Bucket bucket;
    bucket.length = VariableSizeObjectStore::overheadPerPage;
    SetObjectProvider objectProvider;

    while (!readers.empty()) {
        size_t minimumReader = -1;
        uint64_t minimumKey = -1;
        for (size_t i = 0; i < readers.size(); i++) {
            assert(readers.at(i).current().key != minimumKey && "Key collision");
            if (readers.at(i).current().key < minimumKey) {
                minimumKey = readers.at(i).current().key;
                minimumReader = i;
            }
        }

        VariableSizeObjectStore::Item item = readers.at(minimumReader).current();
        bucket.length += VariableSizeObjectStore::overheadPerObject + item.length;
        bucket.items.push_back(item);
        objectProvider.lengths.insert(std::make_pair(item.key, item.length));
        objectProvider.pointers.insert(std::make_pair(item.key, reinterpret_cast<char*>(item.userData)));

        if (readers.at(minimumReader).hasMore()) {
            readers.at(minimumReader).next();
        } else {
            readers.erase(readers.begin() + minimumReader);
        }

        // Flush
        if (bucket.length + VariableSizeObjectStore::overheadPerObject >= PageConfig::PAGE_SIZE || readers.empty()) {
            std::cout<<"---"<<std::endl;
            for (auto &item : bucket.items) {
                std::cout<<item.key<<std::endl;
            }

            bucket.items.clear();
            objectProvider.lengths.clear();
            objectProvider.pointers.clear();
            if (bucket.length > PageConfig::PAGE_SIZE) {
                bucket.length -= PageConfig::PAGE_SIZE; // Overlap
            } else {
                bucket.length = 0;
            }
            bucket.length += VariableSizeObjectStore::overheadPerPage;
        }
    }

    return 0;
}