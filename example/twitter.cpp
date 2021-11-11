#include <EliasFanoObjectStore.h>

int main(int argc, char** argv) {
    std::ifstream input("/mnt/DATEN/Lernen/Doktorand/Forschung/EliasFanoVariableSize/twitter/experiment/all.txt");
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
    VariableSizeObjectStore::printSizeHistogram(tweets);

    EliasFanoObjectStore<8> eliasFanoStore(1.0, "tweetdb.txt", O_DIRECT);
    eliasFanoStore.writeToFile(tweets);
    eliasFanoStore.reloadFromFile();
    eliasFanoStore.printConstructionStats();
}
