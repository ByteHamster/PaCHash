#pragma once

#include "QueryHandle.h"

namespace pachash {
/**
 * Can be used to query an object store.
 * Multiple Views can be opened on one single object store to support multi-threaded queries without locking.
 */
template <class ObjectStore, class IoManager>
class ObjectStoreView {
    public:
        ObjectStore *objectStore;
        IoManager ioManager;

        ObjectStoreView(ObjectStore &objectStore, int openFlags, size_t maxSimultaneousRequests)
                : objectStore(&objectStore),
                  ioManager(objectStore.filename, openFlags, maxSimultaneousRequests * objectStore.requiredIosPerQuery()) {
        }

        inline void enqueueQuery(QueryHandle *handle) {
            objectStore->enqueueQuery(handle, &ioManager);
        }

        inline QueryHandle *awaitAny() {
            return objectStore->awaitAny(&ioManager);
        }

        inline QueryHandle *peekAny() {
            return objectStore->peekAny(&ioManager);
        }

        inline void submitQuery(QueryHandle *handle) {
            objectStore->enqueueQuery(handle, &ioManager);
            ioManager.submit();
        }

        inline void submit() {
            ioManager.submit();
        }
};
} // Namespace pachash
