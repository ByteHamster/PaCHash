#include <string>
#include <iostream>
#include <PaCHashObjectStore.h>

struct Object {
    Object(uint64_t key, size_t size) : key(key), size(size) {
    }
    uint64_t key;
    size_t size;
};

int main() {
    constexpr size_t N = 500000;
    constexpr float percentageSmall = 0.20;
    constexpr uint16_t a = 8;

    std::vector<Object> smallKeysAndValues;
    std::vector<Object> largeKeysAndValues;
    std::vector<Object> keysAndValues;

    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<std::mt19937::result_type> distribution(0,~0);

    for (int i = 0; i < N*percentageSmall; i++) {
        smallKeysAndValues.emplace_back(distribution(rng), 256);
    }
    for (int i = 0; i < N*(1.0-percentageSmall); i++) {
        largeKeysAndValues.emplace_back(distribution(rng), 2*4096);
    }
    keysAndValues.insert(keysAndValues.end(), smallKeysAndValues.begin(), smallKeysAndValues.end());
    keysAndValues.insert(keysAndValues.end(), largeKeysAndValues.begin(), largeKeysAndValues.end());

    pachash::PaCHashObjectStore<a> objectStore(1.0, "key_value_store.db", 0);
    auto hashFunction = [](const Object &x) -> pachash::StoreConfig::key_t {
        return x.key;
    };
    auto lengthEx = [](const Object &x) -> size_t {
        return x.size;
    };
    char *dummyContent = new char[largeKeysAndValues.at(0).size + 10];
    auto valueEx = [dummyContent](const Object &x) -> const char * {
        return dummyContent;
    };
    objectStore.writeToFile(keysAndValues.begin(), keysAndValues.end(), hashFunction, lengthEx, valueEx);
    objectStore.reloadFromFile();

    pachash::ObjectStoreView<decltype(objectStore), pachash::PosixIO> objectStoreView(objectStore, 0, 1);
    pachash::QueryHandle *queryHandle = new pachash::QueryHandle(objectStore);

    for (auto & object : smallKeysAndValues) {
        queryHandle->key = object.key;
        objectStoreView.submitQuery(queryHandle);
        objectStoreView.awaitAny();
    }
    std::cout<<"Small:     "<<(double)queryHandle->stats.blocksFetched/smallKeysAndValues.size()<<std::endl;
    std::cout<<" └╴Theory: "<<(1 + 1.0/a + (double)smallKeysAndValues.at(0).size/pachash::StoreConfig::BLOCK_LENGTH)<<std::endl;
    queryHandle->stats.blocksFetched = 0;

    for (auto & object : largeKeysAndValues) {
        queryHandle->key = object.key;
        objectStoreView.submitQuery(queryHandle);
        objectStoreView.awaitAny();
    }
    std::cout<<"Large:     "<<(double)queryHandle->stats.blocksFetched/largeKeysAndValues.size()<<std::endl;
    std::cout<<" └╴Theory: "<<(1 + 1.0/a + (double)largeKeysAndValues.at(0).size/pachash::StoreConfig::BLOCK_LENGTH)<<std::endl;
    queryHandle->stats.blocksFetched = 0;

    for (int i = 0; i < N; i++) {
        queryHandle->key = distribution(rng);
        objectStoreView.submitQuery(queryHandle);
        objectStoreView.awaitAny();
    }
    std::cout<<"Negative:  "<<(double)queryHandle->stats.blocksFetched/N<<std::endl;
    std::cout<<" └╴Theory: "<<(1 + 1.0/a)<<std::endl;
    std::cout<<" └╴Old:    "<<(1 + 1.0/a + (percentageSmall*(double)smallKeysAndValues.at(0).size +
                    (1.0-percentageSmall)*(double)largeKeysAndValues.at(0).size)/4096)<<std::endl;
    queryHandle->stats.blocksFetched = 0;

    delete queryHandle;
    delete[] dummyContent;
    return 0;
}
