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
    pachash::QueryHandle *queryHandle = new pachash::QueryHandle(objectStore);

    queryHandle->prepare("Key2");
    objectStoreView.submitQuery(queryHandle);

    // This is actually the same object as before,
    // but we could also have submitted multiple handles before calling awaitAny().
    pachash::QueryHandle *returnedHandle = objectStoreView.awaitAny();
    std::cout<<"Retrieved: "<<std::string(returnedHandle->resultPtr, returnedHandle->length)<<std::endl;

    delete queryHandle;
    return 0;
}
