#include <string>
#include <iostream>
#include <IoManager.h>
#include <EliasFanoObjectStore.h>

int main() {
    std::vector<std::pair<std::string, std::string>> keysAndValues;
    keysAndValues.emplace_back("Key1", "Value1");
    keysAndValues.emplace_back("Key2", "Value2");
    keysAndValues.emplace_back("Key3", "Value3");

    EliasFanoObjectStore<8> eliasFanoStore(1.0, "key_value_store.txt", 0);
    eliasFanoStore.writeToFile(keysAndValues);
    eliasFanoStore.reloadFromFile();

    ObjectStoreView<EliasFanoObjectStore<8>, PosixIO> objectStoreView(eliasFanoStore, 0, 1);
    VariableSizeObjectStore::QueryHandle queryHandle;
    queryHandle.buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[eliasFanoStore.requiredBufferPerQuery()];

    queryHandle.prepare("Key2");
    objectStoreView.submitQuery(&queryHandle);
    objectStoreView.awaitAny(); // Only one query, so this returns the same handle again
    std::cout<<"Retrieved: "<<std::string(queryHandle.resultPtr, queryHandle.length)<<std::endl;

    delete[] queryHandle.buffer;
    return 0;
}
