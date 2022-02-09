#include <chrono>
#include <thread>
#include <IoManager.h>
#include <PaCHashObjectStore.h>
#include <Merge.h>
#include <tlx/cmdline_parser.hpp>

void benchmarkMerge(std::vector<std::string> &inputFiles, std::string &outputFile) {
    auto time1 = std::chrono::high_resolution_clock::now();
    std::cout << "# Merging input files: ";
    for (const std::string& inputFile : inputFiles) {
        std::cout << inputFile << " ";
    }
    std::cout<<std::endl;

    pachash::merge(inputFiles, outputFile);

    auto time2 = std::chrono::high_resolution_clock::now();
    pachash::LOG("Flushing");
    sync();
    pachash::LOG(nullptr);
    auto time3 = std::chrono::high_resolution_clock::now();

    size_t space = pachash::filesize(outputFile);
    size_t time = std::chrono::duration_cast<std::chrono::milliseconds >(time3 - time1).count();
    std::cout << "Merging " << pachash::prettyBytes(space) << " completed in " << time << " ms ("
              << pachash::prettyBytes(1000.0 * space / time) << "/s)" << std::endl;
    std::cout << "RESULT"
              << " files=" << inputFiles.size()
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

    for (size_t i = 0; i < iterations; i++) {
        benchmarkMerge(inputFiles, outputFile);
    }
    return 0;
}