#include <string>
#include <iostream>
#include <IoManager.h>
#include <EliasFanoIndexing.h>

class ExampleObjectProvider : public ObjectProvider {
    private:
        std::string tempString;
    public:
        [[nodiscard]] size_t getLength(uint64_t key) final {
            return ("This is the value for key " + std::to_string(key)).length();
        }

        [[nodiscard]] const char *getValue(uint64_t key) final {
            tempString = "This is the value for key " + std::to_string(key);
            return tempString.c_str();
        }
};

int main() {
    std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);

    std::vector<uint64_t> keys;
    for (size_t i = 0; i < 100; i++) {
        keys.emplace_back(dist(generator));
    }

    EliasFanoIndexing<8> eliasFanoStore("key_value_store.txt");
    ExampleObjectProvider objectProvider;
    eliasFanoStore.writeToFile(keys, objectProvider);
    eliasFanoStore.reloadFromFile();

    for (int i = 0; i < 10; i++) {
        std::vector<uint64_t> queryKeys;
        queryKeys.push_back(keys.at(rand() % keys.size()));
        auto result = eliasFanoStore.query(queryKeys);
        const auto& [length, valuePtr] = result.at(0);
        std::cout<<"Retrieved: "<<std::string(valuePtr, length)<<std::endl;
    }

    return 0;
}
