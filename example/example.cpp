#include <string>
#include <iostream>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>

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

    EliasFanoObjectStore<8> eliasFanoStore(1.0, "key_value_store.txt");
    ExampleObjectProvider objectProvider;
    eliasFanoStore.writeToFile(keys, objectProvider);
    eliasFanoStore.reloadFromFile();

    auto queryHandle = eliasFanoStore.newQueryHandle(1);
    for (int i = 0; i < 10; i++) {
        queryHandle->keys.at(0) = keys.at(rand() % keys.size());
        queryHandle->submit();
        queryHandle->awaitCompletion();
        char *content = queryHandle->resultPointers.at(0);
        size_t length = queryHandle->resultLengths.at(0);
        std::cout<<"Retrieved: "<<std::string(content, length)<<std::endl;
    }

    return 0;
}
