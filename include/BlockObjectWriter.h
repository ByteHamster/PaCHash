#pragma once

class BlockObjectWriter {
    public:
        template <typename ValueExtractor>
        static void writeBlocks(const char *filename, std::vector<VariableSizeObjectStore::Block> blocks, ValueExtractor valueExtractor) {
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

            VariableSizeObjectStore::Item firstMetadataItem = {0, sizeof(VariableSizeObjectStore::MetadataObjectType), 0};
            blocks.at(0).items.insert(blocks.at(0).items.begin(), firstMetadataItem);

            for (size_t blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                VariableSizeObjectStore::Block &block = blocks.at(blockIdx);
                VariableSizeObjectStore::BlockStorage storage =
                        VariableSizeObjectStore::BlockStorage::init(file + blockIdx * StoreConfig::BLOCK_LENGTH, 0, block.items.size());

                uint16_t numObjectsInBlock = block.items.size();

                for (size_t i = 0; i < numObjectsInBlock; i++) {
                    storage.lengths[i] = block.items.at(i).length;
                }
                for (size_t i = 0; i < numObjectsInBlock; i++) {
                    storage.keys[i] = block.items.at(i).key;
                }

                char *writePosition = storage.objectsStart;
                for (size_t i = 0; i < numObjectsInBlock; i++) {
                    VariableSizeObjectStore::Item &item = block.items.at(i);
                    if (item.key == 0) {
                        VariableSizeObjectStore::MetadataObjectType metadataObject = numBlocks;
                        memcpy(writePosition, &metadataObject, sizeof(VariableSizeObjectStore::MetadataObjectType));
                    } else {
                        const char *objectContent = valueExtractor(item.key);
                        assert(item.length <= StoreConfig::MAX_OBJECT_SIZE);
                        memcpy(writePosition, objectContent, item.length);
                    }
                    writePosition += storage.lengths[i];
                }
                VariableSizeObjectStore::LOG("Writing", blockIdx, numBlocks);
            }
            VariableSizeObjectStore::BlockStorage::init(file + numBlocks * StoreConfig::BLOCK_LENGTH, 0, 0);
            VariableSizeObjectStore::LOG("Flushing and closing file");
            munmap(file, fileSize);
            close(fd);
        }
};
