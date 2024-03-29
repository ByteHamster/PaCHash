#pragma once

namespace pachash {
class BlockObjectWriter {
    public:
        struct Item {
            StoreConfig::key_t key = 0;
            size_t length = 0;
            uint64_t hashFunctionIndex = 0;
            uint64_t currentHash = 0;
            void *ptr = nullptr;
        };
        struct Block {
            std::vector<Item> items;
            size_t length = 0;
        };

        template <typename ValueExtractor, typename U>
        static void writeBlocks(const char *filename, int fileFlags, size_t maxSize,
                                std::vector<Block> blocks, ValueExtractor valueExtractor, uint16_t type) {
            size_t numBlocks = blocks.size();

            // If the file does not exist or is a partition, truncating fails, so we silently ignore the result
            uint64_t fileSize = (numBlocks + 1) * StoreConfig::BLOCK_LENGTH;
            int result = truncate(filename, fileSize);
            (void) result;

            size_t blocksPerBatch = 250;
            char *buffer1 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH))
                    char[blocksPerBatch * StoreConfig::BLOCK_LENGTH];
            char *buffer2 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH))
                    char[blocksPerBatch * StoreConfig::BLOCK_LENGTH];
            memset(buffer1, 0, blocksPerBatch * StoreConfig::BLOCK_LENGTH);
            memset(buffer2, 0, blocksPerBatch * StoreConfig::BLOCK_LENGTH);

            #ifdef HAS_LIBURING
            UringIO ioManager(filename, fileFlags | O_RDWR | O_CREAT, 2);
            #else
            PosixIO ioManager(filename, fileFlags | O_RDWR | O_CREAT, 2);
            #endif

            Item firstMetadataItem = {0,sizeof(VariableSizeObjectStore::StoreMetadata), 0};
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
                            buffer1 + (blockIdx % blocksPerBatch) * StoreConfig::BLOCK_LENGTH, 0);
                    continue;
                }
                Block &block = blocks.at(blockIdx);
                VariableSizeObjectStore::BlockStorage storage = VariableSizeObjectStore::BlockStorage::init(
                        buffer1 + (blockIdx % blocksPerBatch) * StoreConfig::BLOCK_LENGTH, block.items.size());

                size_t writeOffset = 0;
                size_t i = 0;
                assert(block.items.size() < StoreConfig::num_objects_t(~0) && "Increase StoreConfig::num_objects_t size");
                for (const Item &item : block.items) {
                     if (i > 0) { // First offset is always 0
                        storage.offsets[i - 1] = writeOffset;
                        if (i == block.items.size() - 1) {
                            // Last item also stores its end offset
                            storage.offsets[i] = writeOffset + item.length;
                        }
                    }
                    storage.keys[i] = item.key;

                    if (item.key == 0) {
                        VariableSizeObjectStore::StoreMetadata metadata;
                        metadata.numBlocks = numBlocks;
                        metadata.maxSize = maxSize;
                        metadata.type = type;
                        memcpy(storage.blockStart + writeOffset, &metadata, sizeof(VariableSizeObjectStore::StoreMetadata));
                    } else {
                        const char *objectContent = valueExtractor(*((U*)item.ptr));
                        memcpy(storage.blockStart + writeOffset, objectContent, item.length);
                    }
                    writeOffset += item.length;
                    i++;
                }
                LOG("Writing", blockIdx, numBlocks);
            }

            if (numBlocks % blocksPerBatch != 0) {
                // Write block that was started but not flushed
                ioManager.enqueueWrite(buffer1, (numBlocks - (numBlocks % blocksPerBatch)) * StoreConfig::BLOCK_LENGTH,
                                       (numBlocks % blocksPerBatch) * StoreConfig::BLOCK_LENGTH, 0);
                ioManager.submit();
                ioManager.awaitAny();
            }
            if (numBlocks >= blocksPerBatch) {
                ioManager.awaitAny(); // Last block submitted in the loop
            }

            delete[] buffer1;
            delete[] buffer2;
        }
};

} // Namespace pachash
