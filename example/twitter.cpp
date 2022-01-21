#include <PaCHashObjectStore.h>
#include <SeparatorObjectStore.h>
#include <ParallelCuckooObjectStore.h>
#include <tlx/cmdline_parser.hpp>

/**
 * Rather basic construction example. Reads tweets from a file with format:
 * <tweet id> <tweet content>\n
 * <tweet id> <tweet content>\n
 *
 * Data is first all copied into a std::vector and then passed to the object store.
 * Note that using the default key extractor like this is rather slow
 * because we need to re-calculate the hash of each pair multiple times.
 */
int main(int argc, char** argv) {
    std::string inputFile = "twitter-stream-2021-08-01.txt";
    std::string outputFile = "key_value_store.db";
    std::string type = "pachash";
    bool cachedIo = false;

    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", inputFile, "Tweet input file");
    cmd.add_string('o', "output_file", outputFile, "Object store file");
    cmd.add_string('t', "type", type, "Object store type to generate");
    cmd.add_bool('c', "cached_io", cachedIo, "Use cached instead of direct IO");
    if (!cmd.process(argc, argv)) {
        return 1;
    }

    std::ifstream input(inputFile);
    if (errno != 0) {
        std::cerr<<strerror(errno)<<std::endl;
        return 1;
    }
    std::string line;
    std::vector<std::pair<std::string, std::string>> tweets;
    while (std::getline(input,line)) {
        size_t spacePosition = line.find_first_of(' ');
        tweets.emplace_back(line.substr(0, spacePosition), line.substr(spacePosition+1));
        if (tweets.size() % 12123 == 0) {
            std::cout<<"\r\033[KTweets read: "<<tweets.size()<<std::flush;
        }
    }
    std::cout<<"\r\033[KTweets read: "<<tweets.size()<<std::endl;

    pachash::VariableSizeObjectStore *objectStore;
    if (type == "pachash") {
        auto pachashStore = new pachash::PaCHashObjectStore<8>(1.0, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        pachashStore->writeToFile(tweets);
        objectStore = pachashStore;
    } else if (type == "cuckoo") {
        auto cuckooStore = new pachash::ParallelCuckooObjectStore(0.95, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        cuckooStore->writeToFile(tweets);
        objectStore = cuckooStore;
    } else if (type == "separator") {
        auto separatorStore = new pachash::SeparatorObjectStore<6>(0.95, outputFile.c_str(), cachedIo ? 0 : O_DIRECT);
        separatorStore->writeToFile(tweets);
        objectStore = separatorStore;
    } else {
        throw std::logic_error("Invalid value for command line argument 'type'.");
    }

    objectStore->reloadFromFile();
    objectStore->printSizeHistogram(tweets);
    objectStore->printConstructionStats();
    delete objectStore;
    return 0;
}
