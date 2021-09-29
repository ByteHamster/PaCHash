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
        int fd;
    public:
        size_t numBlocks;
        bool completed = false;
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
            munmap(file, numBlocks * PageConfig::PAGE_SIZE);
            close(fd);
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
    size_t totalBlocks = 0;
    for (const std::string& inputFile : inputFiles) {
        readers.emplace_back(inputFile.c_str());
        totalBlocks += readers.back().numBlocks;
    }

    int fd = open(outputFile.c_str(), O_RDWR | O_CREAT, 0600);
    if (fd < 0) {
        std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
        exit(1);
    }

    size_t fileSize = (totalBlocks+1) * PageConfig::PAGE_SIZE;
    ftruncate(fd, fileSize);
    char *output = static_cast<char *>(mmap(nullptr, fileSize, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0));
    assert(output != MAP_FAILED);

    VariableSizeObjectStore::Bucket currentBucket;
    currentBucket.length = VariableSizeObjectStore::overheadPerPage;
    SetObjectProvider currentObjectProvider;
    VariableSizeObjectStore::Bucket previousBucket;
    SetObjectProvider previousObjectProvider;
    size_t bucketsGenerated = 0;
    size_t offset = 0;
    size_t readersCompleted = 0;

    while (readersCompleted < readers.size()) {
        size_t minimumReader = -1;
        uint64_t minimumKey = -1;
        for (size_t i = 0; i < readers.size(); i++) {
            if (readers.at(i).completed) {
                continue;
            }
            assert(readers.at(i).current().key != minimumKey && "Key collision");
            if (readers.at(i).current().key < minimumKey) {
                minimumKey = readers.at(i).current().key;
                minimumReader = i;
            }
        }

        VariableSizeObjectStore::Item item = readers.at(minimumReader).current();
        currentBucket.length += VariableSizeObjectStore::overheadPerObject + item.length;
        currentBucket.items.push_back(item);
        currentObjectProvider.lengths.insert(std::make_pair(item.key, item.length));
        currentObjectProvider.pointers.insert(std::make_pair(item.key, reinterpret_cast<char*>(item.userData)));

        if (readers.at(minimumReader).hasMore()) {
            readers.at(minimumReader).next();
        } else {
            readersCompleted++;
            readers.at(minimumReader).completed = true;
        }

        // Flush
        if (currentBucket.length + VariableSizeObjectStore::overheadPerObject >= PageConfig::PAGE_SIZE || readers.empty()) {
            if (bucketsGenerated > 0) {
                auto storage = VariableSizeObjectStore::BlockStorage::init(
                        output + (bucketsGenerated-1)*PageConfig::PAGE_SIZE, offset, previousBucket.items.size());
                storage.pageStart[PageConfig::PAGE_SIZE - 1] = 42;
                offset = VariableSizeObjectStore::writeBucket(previousBucket, storage, previousObjectProvider, true, currentBucket.items.size());
                std::cout<<"---"<<std::endl;
                for (auto &item : previousBucket.items) {
                    std::cout<<item.key<<std::endl;
                }
            }

            previousBucket = currentBucket;
            previousObjectProvider = currentObjectProvider;
            currentBucket.items.clear();
            currentObjectProvider.lengths.clear();
            currentObjectProvider.pointers.clear();
            if (currentBucket.length > PageConfig::PAGE_SIZE) {
                currentBucket.length -= PageConfig::PAGE_SIZE; // Overlap
            } else {
                currentBucket.length = 0;
            }
            currentBucket.length += VariableSizeObjectStore::overheadPerPage;
            bucketsGenerated++;
        }
    }

    auto storage = VariableSizeObjectStore::BlockStorage::init(
            output + (bucketsGenerated)*PageConfig::PAGE_SIZE, offset, previousBucket.items.size());
    offset = VariableSizeObjectStore::writeBucket(previousBucket, storage, previousObjectProvider, true, currentBucket.items.size());

    std::cout<<"---"<<std::endl;
    for (auto &item : previousBucket.items) {
        std::cout<<item.key<<std::endl;
    }

    auto storage2 = VariableSizeObjectStore::BlockStorage::init(
            output + (bucketsGenerated+1)*PageConfig::PAGE_SIZE, offset, currentBucket.items.size());
    offset = VariableSizeObjectStore::writeBucket(currentBucket, storage, currentObjectProvider, true, 0);

    std::cout<<"---"<<std::endl;
    for (auto &item : currentBucket.items) {
        std::cout<<item.key<<std::endl;
    }

    munmap(output, fileSize);
    close(fd);
    return 0;
}