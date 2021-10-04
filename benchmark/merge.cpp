#include <chrono>
#include <thread>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>
#include <LinearObjectWriter.h>
#include <tlx/cmdline_parser.hpp>

class LinearObjectReader {
    private:
        size_t currentBlock = -1;
        size_t currentElement = -1;
        char *file;
        VariableSizeObjectStore::BlockStorage *block = nullptr;
        int fd;
        // For merging, we keep at most 2 blocks per input file before writing
        char* objectReconstructionBuffers[2] = {};
        size_t roundRobinObjectReconstructionBuffers = 0;
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
            objectReconstructionBuffers[0] = static_cast<char *>(malloc(PageConfig::MAX_OBJECT_SIZE));
            objectReconstructionBuffers[1] = static_cast<char *>(malloc(PageConfig::MAX_OBJECT_SIZE));

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
            return currentBlock < numBlocks - 1 || currentElement + 1 < block->numObjects;
        }

        void next() {
            if (block == nullptr || currentElement + 1 == block->numObjects) {
                currentBlock++;
                currentElement = 0;
                do {
                    delete block;
                    block = new VariableSizeObjectStore::BlockStorage(file + currentBlock * PageConfig::PAGE_SIZE);
                    block->calculateObjectPositions();
                } while(block->numObjects == 0 && currentBlock < numBlocks - 1);
            } else {
                currentElement++;
            }
        }

        uint64_t currentKey() {
            assert(block->numObjects > currentElement);
            return block->keys[currentElement];
        }

        VariableSizeObjectStore::Item prepareCurrent() {
            assert(block->numObjects > currentElement);
            uint64_t key = block->keys[currentElement];
            size_t length =  block->lengths[currentElement];
            char *pointer = block->objects[currentElement];
            size_t spaceLeft = block->tableStart - block->pageStart;
            if (spaceLeft < length) {
                // Must reconstruct object because it overlaps
                char *newPointer = objectReconstructionBuffers[roundRobinObjectReconstructionBuffers];
                roundRobinObjectReconstructionBuffers = (roundRobinObjectReconstructionBuffers + 1) % 2;
                memcpy(newPointer, pointer, spaceLeft);
                VariableSizeObjectStore::BlockStorage nextBlock(block->pageStart + PageConfig::PAGE_SIZE);
                memcpy(newPointer + spaceLeft, nextBlock.pageStart, length - spaceLeft);
                pointer = newPointer;
            }
            return VariableSizeObjectStore::Item {key, length, reinterpret_cast<size_t>(pointer) };
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

    auto time1 = std::chrono::high_resolution_clock::now();
    std::vector<LinearObjectReader> readers;
    readers.reserve(inputFiles.size());
    size_t totalBlocks = 0;
    for (const std::string& inputFile : inputFiles) {
        readers.emplace_back(inputFile.c_str());
        totalBlocks += readers.back().numBlocks;
    }

    LinearObjectWriter writer(outputFile.c_str(), totalBlocks);
    size_t readersCompleted = 0;



    while (readersCompleted < readers.size()) {
        size_t minimumReader = -1;
        uint64_t minimumKey = -1;
        for (size_t i = 0; i < readers.size(); i++) {
            if (readers.at(i).completed) {
                continue;
            }
            assert(readers.at(i).currentKey() != minimumKey && "Key collision");
            if (readers.at(i).currentKey() < minimumKey) {
                minimumKey = readers.at(i).currentKey();
                minimumReader = i;
            }
        }

        VariableSizeObjectStore::Item item = readers.at(minimumReader).prepareCurrent();
        assert(item.length < PageConfig::PAGE_SIZE);
        writer.write(item.key, item.length, reinterpret_cast<const char *>(item.userData));

        if (readers.at(minimumReader).hasMore()) {
            readers.at(minimumReader).next();
        }
        if (!readers.at(minimumReader).hasMore()) {
            readersCompleted++;
            readers.at(minimumReader).completed = true;
        }
        VariableSizeObjectStore::LOG("Merging", writer.bucketsGenerated-1, totalBlocks);
    }

    auto time2 = std::chrono::high_resolution_clock::now();
    writer.close();
    VariableSizeObjectStore::LOG("Flushing");
    system("sync");
    VariableSizeObjectStore::LOG(nullptr);
    auto time3 = std::chrono::high_resolution_clock::now();

    std::cout<<"Merging completed"<<std::endl;
    std::cout<<"Time merging: "<<std::chrono::duration_cast<std::chrono::milliseconds>(time2 - time1).count()<<std::endl;
    std::cout<<"Time sync:    "<<std::chrono::duration_cast<std::chrono::milliseconds>(time3 - time2).count()<<std::endl;
    return 0;
}