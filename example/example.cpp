#include <string>
#include <iostream>
#include <PaCHashObjectStore.h>

/**
 * Most basic example. Constructs an object store and queries a key.
 */
int main() {
    std::vector<std::pair<std::string, std::string>> keysAndValues;
    keysAndValues.emplace_back("Key1", "Value1");
    keysAndValues.emplace_back("Key2", "Value2");
    keysAndValues.emplace_back("Key3", "Value3");

    pachash::PaCHashObjectStore<8> objectStore(1.0, "key_value_store.db", 0);
    objectStore.writeToFile(keysAndValues);
    objectStore.reloadFromFile();

    pachash::ObjectStoreView<pachash::PaCHashObjectStore<8>, pachash::PosixIO> objectStoreView(objectStore, 0, 1);
    pachash::VariableSizeObjectStore::QueryHandle queryHandle;
    queryHandle.buffer = new (std::align_val_t(pachash::StoreConfig::BLOCK_LENGTH)) char[objectStore.requiredBufferPerQuery()];

    queryHandle.prepare("Key2");
    objectStoreView.submitQuery(&queryHandle);

    pachash::VariableSizeObjectStore::QueryHandle *returnedHandle = objectStoreView.awaitAny();
    std::cout<<"Retrieved: "<<std::string(returnedHandle->resultPtr, returnedHandle->length)<<std::endl;

    delete[] queryHandle.buffer;
    return 0;
}
