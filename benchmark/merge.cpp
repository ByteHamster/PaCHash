#include <chrono>
#include <thread>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>
#include <LinearObjectReader.h>
#include <LinearObjectWriter.h>
#include <tlx/cmdline_parser.hpp>

void benchmarkMerge(std::vector<std::string> &inputFiles, std::string &outputFile) {
    auto time1 = std::chrono::high_resolution_clock::now();
    std::vector<LinearObjectReader> readers;
    readers.reserve(inputFiles.size());
    size_t totalBlocks = 0;
    std::cout << "# Merging input files: ";
    for (const std::string& inputFile : inputFiles) {
        readers.emplace_back(inputFile.c_str(), O_DIRECT);
        totalBlocks += readers.back().numBlocks;
        std::cout << inputFile << " ";
    }
    std::cout<<std::endl;

    LinearObjectWriter writer(outputFile.c_str(), O_DIRECT);
    size_t readersCompleted = 0;
    size_t totalObjects = 0;

    while (readersCompleted < readers.size()) {
        size_t minimumReader = -1;
        size_t minimumLength = -1;
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
                minimumLength = reader.currentLength();
            }
        }

        LinearObjectReader &minReader = readers[minimumReader];
        writer.write(minimumKey, minimumLength, minReader.currentContent());
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