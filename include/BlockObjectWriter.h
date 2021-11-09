#pragma once
#include <list>

class BlockObjectWriter {
    public:
        struct Item {
            StoreConfig::key_t key = 0;
            StoreConfig::length_t length = 0;
            uint64_t userData = 0; // Eg. number of hash function
            void *ptr = nullptr;
        };
        struct Block {
            std::list<Item> items;
            size_t length = 0;
        };

        template <typename ValueExtractor>
        static void writeBlocks(const char *filename, std::vector<Block> blocks, ValueExtractor valueExtractor) {
            size_t numBlocks = blocks.size();

            int fd = open(filename, O_RDWR | O_CREAT, 0600);
            if (fd < 0) {
                std::cerr<<"Error opening output file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            uint64_t fileSize = (numBlocks + 1) * StoreConfig::BLOCK_LENGTH;
            if (ftruncate(fd, fileSize) < 0) {
                std::cerr<<"ftruncate: "<<strerror(errno)<<". If this is a partition, it can be ignored."<<std::endl;
            }
            char *file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
            if (file == MAP_FAILED) {
                std::cerr<<"Map output file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            madvise(file, fileSize, MADV_SEQUENTIAL);

            Item firstMetadataItem = {0, sizeof(VariableSizeObjectStore::MetadataObjectType), 0};
            blocks.at(0).items.insert(blocks.at(0).items.begin(), firstMetadataItem);

            for (size_t blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                Block &block = blocks.at(blockIdx);
                VariableSizeObjectStore::BlockStorage storage =
                        VariableSizeObjectStore::BlockStorage::init(file + blockIdx * StoreConfig::BLOCK_LENGTH, 0, block.items.size());

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
            VariableSizeObjectStore::BlockStorage::init(file + numBlocks * StoreConfig::BLOCK_LENGTH, 0, 0);
            VariableSizeObjectStore::LOG("Flushing and closing file");
            munmap(file, fileSize);
            sync();
            close(fd);
        }
};
