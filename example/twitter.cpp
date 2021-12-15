#include <PaCHashObjectStore.h>
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

    tlx::CmdlineParser cmd;
    cmd.add_string('i', "input_file", inputFile, "Tweet input file");
    cmd.add_string('o', "output_file", outputFile, "Object store file");
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

    pachash::PaCHashObjectStore<8> objectStore(1.0, outputFile.c_str(), O_DIRECT);
    objectStore.writeToFile(tweets);
    objectStore.reloadFromFile();
    objectStore.printSizeHistogram(tweets);
    objectStore.printConstructionStats();
    return 0;
}
