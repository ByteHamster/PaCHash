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

    VariableSizeObjectStore::QueryHandle queryHandle;
    queryHandle.buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, eliasFanoStore.requiredBufferPerQuery()));
    for (int i = 0; i < 10; i++) {
        queryHandle.key = keys.at(rand() % keys.size());
        eliasFanoStore.submitQuery(&queryHandle);
        eliasFanoStore.awaitAny(); // Only one query, so this returns the same handle again
        std::cout<<"Retrieved: "<<std::string(queryHandle.resultPtr, queryHandle.length)<<std::endl;
    }
    free(queryHandle.buffer);

    return 0;
}
