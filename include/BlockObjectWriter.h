#pragma once

class BlockObjectWriter {
    public:
        struct Item {
            StoreConfig::key_t key = 0;
            StoreConfig::length_t length = 0;
            uint64_t userData = 0; // Eg. number of hash function
            void *ptr = nullptr;
        };
        struct SimpleBlock {
            std::vector<Item> items;
            size_t length = 0;
        };

        template <typename ValueExtractor, typename Block>
        static void writeBlocks(const char *filename, int fileFlags,
                                                       std::vector<Block> blocks, ValueExtractor valueExtractor) {
            size_t numBlocks = blocks.size();

            uint64_t fileSize = (numBlocks + 1) * StoreConfig::BLOCK_LENGTH;
            if (truncate(filename, fileSize) < 0) {
                std::cerr<<"ftruncate: "<<strerror(errno)<<". If this is a partition, it can be ignored."<<std::endl;
            }

            size_t blocksPerBatch = 250;
            char *buffer1 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[blocksPerBatch * StoreConfig::BLOCK_LENGTH];
            char *buffer2 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[blocksPerBatch * StoreConfig::BLOCK_LENGTH];
            #ifdef HAS_LIBURING
            UringIO ioManager(filename, fileFlags | O_RDWR | O_CREAT, 2);
            #else
            PosixIO ioManager(filename, fileFlags | O_RDWR | O_CREAT, 2);
            #endif

            Item firstMetadataItem = {0, sizeof(VariableSizeObjectStore::MetadataObjectType), 0};
            blocks.at(0).items.insert(blocks.at(0).items.begin(), firstMetadataItem);

            for (size_t blockIdx = 0; blockIdx <= numBlocks; blockIdx++) {
                if (blockIdx % blocksPerBatch == 0 && blockIdx != 0) {
                    if (blockIdx != blocksPerBatch) {
                        ioManager.awaitAny();
                    }
                    std::swap(buffer1, buffer2);
                    ioManager.enqueueWrite(buffer2, (blockIdx - blocksPerBatch) * StoreConfig::BLOCK_LENGTH,
                                           blocksPerBatch*StoreConfig::BLOCK_LENGTH, 0);
                    ioManager.submit();
                }
                if (blockIdx == numBlocks) {
                    VariableSizeObjectStore::BlockStorage::init(
                            buffer1 + (blockIdx % blocksPerBatch) * StoreConfig::BLOCK_LENGTH, 0, 0);
                    continue;
                }
                Block &block = blocks.at(blockIdx);
                VariableSizeObjectStore::BlockStorage storage = VariableSizeObjectStore::BlockStorage::init(
                        buffer1 + (blockIdx % blocksPerBatch) * StoreConfig::BLOCK_LENGTH, 0, block.items.size());

                char *writePosition = storage.objectsStart;
                size_t i = 0;
                for (const Item &item : block.items) {
                    storage.lengths[i] = item.length;
                    storage.keys[i] = item.key;

                    if (item.key == 0) {
                        VariableSizeObjectStore::MetadataObjectType metadataObject = numBlocks;
                        memcpy(writePosition, &metadataObject, sizeof(VariableSizeObjectStore::MetadataObjectType));
                    } else {
                        const char *objectContent = valueExtractor(item.key);
                        assert(item.length <= StoreConfig::MAX_OBJECT_SIZE);
                        memcpy(writePosition, objectContent, item.length);
                    }
                    writePosition += item.length;
                    i++;
                }
                VariableSizeObjectStore::LOG("Writing", blockIdx, numBlocks);
            }

            ioManager.enqueueWrite(buffer1, (numBlocks - (numBlocks % blocksPerBatch)) * StoreConfig::BLOCK_LENGTH,
                                   (numBlocks % blocksPerBatch)*StoreConfig::BLOCK_LENGTH, 0);
            ioManager.submit();
            ioManager.awaitAny(); // Both buffers
            ioManager.awaitAny();

            delete[] buffer1;
            delete[] buffer2;
        }
};
