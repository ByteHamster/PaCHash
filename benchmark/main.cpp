#include "VariableSizeObjectStoreTest.h"
#include "IoManager.h"

int main() {
    testVariableSizeObjectStores<PosixIO<O_DIRECT | O_SYNC>>(1e7, 256, 0.98, 1);
    testVariableSizeObjectStores<PosixAIO<O_DIRECT | O_SYNC>>(1e7, 256, 0.98, 1);
    testVariableSizeObjectStores<UringIO<>>(1e7, 256, 0.98, 1);
    return 0;
}
