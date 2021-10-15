#include <chrono>
#include <thread>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>
#include <LinearObjectWriter.h>
#include <tlx/cmdline_parser.hpp>

class LinearObjectReader {
    public:
        size_t numBlocks;
    private:
        size_t currentBlock = 0;
        size_t currentElement = 0;
        UringDoubleBufferBlockIterator blockIterator;
        VariableSizeObjectStore::BlockStorage *block = nullptr;
        char* objectReconstructionBuffer = nullptr;
    public:
        bool completed = false;
        explicit LinearObjectReader(const char *filename)
                : numBlocks(EliasFanoObjectStore<8>::readSpecialObject0(filename)),
                  blockIterator(filename, numBlocks, 250) {
            objectReconstructionBuffer = new char[PageConfig::MAX_OBJECT_SIZE];
            block = new VariableSizeObjectStore::BlockStorage(blockIterator.bucketContent());
            block->calculateObjectPositions();
            next(); // Skip pseudo object 0
        }

        ~LinearObjectReader() {
            delete block;
            block = nullptr;
            delete[] objectReconstructionBuffer;
        }

        [[nodiscard]] bool hasEnded() const {
            return currentBlock >= numBlocks;
        }

        void next() {
            assert(!hasEnded());
            if (block != nullptr && currentElement + 1 < block->numObjects) {
                currentElement++;
            } else {
                if (currentBlock + 1 >= numBlocks) {
                    currentBlock++;
                    return;
                }
                currentElement = 0;
                do {
                    delete block;
                    currentBlock++;
                    blockIterator.next();
                    block = new VariableSizeObjectStore::BlockStorage(blockIterator.bucketContent());
                    block->calculateObjectPositions();
                } while (block->numObjects == 0 && currentBlock < numBlocks - 1);
                if (currentBlock == numBlocks - 1 && block->numObjects == 0) {
                    currentBlock++; // Indicator for "ended"
                }
            }
        }

        uint64_t currentKey() {
            assert(currentElement < block->numObjects);
            return block->keys[currentElement];
        }

        uint16_t currentLength() {
            assert(currentElement < block->numObjects);
            return block->lengths[currentElement];
        }

        char *currentContent() {
            assert(currentElement < block->numObjects);
            size_t length =  block->lengths[currentElement];
            char *pointer = block->objects[currentElement];
            size_t spaceLeft = block->tableStart - block->objects[currentElement];
            if (spaceLeft >= length) {
                // No copying needed
                return pointer;
            }

            memcpy(objectReconstructionBuffer, pointer, spaceLeft);

            char *page = block->pageStart + PageConfig::PAGE_SIZE;
            size_t reconstructed = spaceLeft;
            char *readTo = objectReconstructionBuffer + spaceLeft;
            while (reconstructed < length) {
                VariableSizeObjectStore::BlockStorage readBlock(page); // TODO: This could be in a range that is not read yet
                size_t spaceInNextBucket = (readBlock.tableStart - readBlock.pageStart);
                assert(spaceInNextBucket <= PageConfig::PAGE_SIZE);
                size_t spaceToCopy = std::min(length - reconstructed, spaceInNextBucket);
                assert(spaceToCopy > 0 && spaceToCopy <= PageConfig::MAX_OBJECT_SIZE);
                memcpy(readTo, readBlock.pageStart, spaceToCopy);
                reconstructed += spaceToCopy;
                readTo += spaceToCopy;
                page += PageConfig::PAGE_SIZE;
                assert(reconstructed <= PageConfig::MAX_OBJECT_SIZE);
            }
            return objectReconstructionBuffer;
        }
};

void benchmarkMerge(std::vector<std::string> &inputFiles, std::string &outputFile) {
    auto time1 = std::chrono::high_resolution_clock::now();
    std::vector<LinearObjectReader> readers;
    readers.reserve(inputFiles.size());
    size_t totalBlocks = 0;
    std::cout << "# Merging input files: ";
    for (const std::string& inputFile : inputFiles) {
        readers.emplace_back(inputFile.c_str());
        totalBlocks += readers.back().numBlocks;
        std::cout << inputFile << " ";
    }
    std::cout<<std::endl;

    LinearObjectWriter writer(outputFile.c_str(), totalBlocks);
    size_t readersCompleted = 0;
    size_t totalObjects = 0;

    while (readersCompleted < readers.size()) {
        size_t minimumReader = -1;
        uint64_t minimumKey = ~0;
        for (size_t i = 0; i < readers.size(); i++) {
            LinearObjectReader &reader = readers[i];
            if (reader.completed) {
                continue;
            }
            uint64_t currentKey = reader.currentKey();
            assert(currentKey != minimumKey && "Key collision");
            if (currentKey < minimumKey) {
                minimumKey = currentKey;
                minimumReader = i;
            }
        }

        LinearObjectReader &minReader = readers[minimumReader];
        writer.write(minReader.currentKey(), minReader.currentLength(), minReader.currentContent());
        totalObjects++;

        minReader.next();
        if (minReader.hasEnded()) {
            readersCompleted++;
            minReader.completed = true;
        }
        VariableSizeObjectStore::LOG("Merging", writer.bucketsGenerated-1, totalBlocks);
    }

    auto time2 = std::chrono::high_resolution_clock::now();
    writer.close();
    VariableSizeObjectStore::LOG("Flushing");
    int result = system("sync");
    (void) result;
    VariableSizeObjectStore::LOG(nullptr);
    auto time3 = std::chrono::high_resolution_clock::now();

    std::cout << "Merging completed" << std::endl; // Needed to avoid \r in front of RESULT line
    std::cout << "RESULT"
              << " files=" << readers.size()
              << " objects=" << totalObjects
              << " merge=" << std::chrono::duration_cast<std::chrono::nanoseconds>(time2 - time1).count()
              << " sync=" << std::chrono::duration_cast<std::chrono::nanoseconds>(time3 - time2).count()
              << std::endl;
}

int main(int argc, char** argv) {
    std::vector<std::string> inputFiles;
    std::string outputFile;
    size_t iterations = 1;

    tlx::CmdlineParser cmd;
    cmd.add_stringlist('i', "input_file", inputFiles, "Input file that should be merged. Can be specified multiple times");
    cmd.add_string('o', "output_file", outputFile, "File to write the merged data structure to");
    cmd.add_size_t('n', "iterations", iterations, "Merge multiple times");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    if (inputFiles.empty() || outputFile.empty()) {
        std::cerr<<"Need input and output files"<<std::endl;
        cmd.print_usage();
        return 1;
    }

    for (int i = 0; i < iterations; i++) {
        benchmarkMerge(inputFiles, outputFile);
    }
    return 0;
}