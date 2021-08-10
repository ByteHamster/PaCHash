#include <string>
#include <iostream>
#include <IoManager.h>
#include <EliasFanoIndexing.h>

int main() {
    std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);

    std::vector<std::pair<uint64_t, size_t>> keysAndLengths;
    size_t totalLength = 0;
    for (size_t i = 0; i < 100; i++) {
        uint64_t key = dist(generator);
        size_t length = ("This is the value for key " + std::to_string(key)).length();
        totalLength += length;
        keysAndLengths.emplace_back(key, length);
    }

    EliasFanoIndexing<8> eliasFanoStore(keysAndLengths.size(),
                   totalLength/keysAndLengths.size(), "key_value_store.txt");
    std::string temp;
    eliasFanoStore.generateInputData(keysAndLengths, [&] (uint64_t key) {
        temp = "This is the value for key " + std::to_string(key);
        return temp.c_str();
    });
    eliasFanoStore.reloadInputDataFromFile();

    for (int i = 0; i < 10; i++) {
        std::vector<uint64_t> keys;
        keys.push_back(keysAndLengths.at(rand() % keysAndLengths.size()).first);
        auto result = eliasFanoStore.query(keys);
        const auto& [length, valuePtr] = result.at(0);

        std::cout<<"Retrieved: "<<std::string(valuePtr, length)<<std::endl;
    }

    return 0;
}
