#pragma once

namespace pachash {
class LinearObjectWriter {
    private:
        static constexpr size_t BLOCK_FLUSH = 250;
        int fd;
        size_t numObjectsOnPage = 0;
        StoreConfig::key_t keys[StoreConfig::BLOCK_LENGTH / VariableSizeObjectStore::overheadPerObject] = {0};
        StoreConfig::offset_t offsets[StoreConfig::BLOCK_LENGTH / VariableSizeObjectStore::overheadPerObject] = {0};
        StoreConfig::offset_t spaceLeftOnBlock = StoreConfig::BLOCK_LENGTH - VariableSizeObjectStore::overheadPerBlock;
        size_t blockWritingPosition = 0;
        char *currentBlock = nullptr;
        char *buffer1 = nullptr;
        char *buffer2 = nullptr;
        size_t maxSize = 0;
        #ifdef HAS_LIBURING
        UringIO ioManager;
        #else
        PosixIO ioManager;
        #endif
    public:
        size_t blocksGenerated = 0;

        explicit LinearObjectWriter(const char *filename, int flags)
                : ioManager(filename, O_CREAT | flags, 2) {
            fd = open(filename, O_RDWR | O_CREAT | flags, 0666);
            if (fd < 0) {
                throw std::ios_base::failure("Unable to open " + std::string(filename)
                         + ": " + std::string(strerror(errno)));
            }
            buffer1 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH];
            buffer2 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH];
            memset(buffer1, 0, BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH);
            memset(buffer2, 0, BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH);
            currentBlock = buffer1;
            VariableSizeObjectStore::StoreMetadata metadataDummy = {};
            write(0, sizeof(VariableSizeObjectStore::StoreMetadata), reinterpret_cast<const char *>(&metadataDummy));
        }

        ~LinearObjectWriter() {
            delete[] buffer1;
            delete[] buffer2;
        }

        void write(StoreConfig::key_t key, size_t length, const char* content) {
            maxSize = std::max(maxSize, length);
            size_t written = 0;
            keys[numObjectsOnPage] = key;
            offsets[numObjectsOnPage] = blockWritingPosition;
            assert(blockWritingPosition <= StoreConfig::BLOCK_LENGTH);
            numObjectsOnPage++;
            spaceLeftOnBlock -= VariableSizeObjectStore::overheadPerObject;

            do {
                size_t toWrite = std::min(size_t(spaceLeftOnBlock), length - written);
                memcpy(currentBlock + blockWritingPosition, content + written, toWrite);
                blockWritingPosition += toWrite;
                spaceLeftOnBlock -= toWrite;
                written += toWrite;

                if (spaceLeftOnBlock <= VariableSizeObjectStore::overheadPerObject) {
                    // No more object fits on the page.
                    writeTable(false, spaceLeftOnBlock);
                }
            } while (written < length);
        }

        void writeTable(bool forceFlush, char emptySpace) {
            assert(blockWritingPosition <= StoreConfig::BLOCK_LENGTH);
            assert(numObjectsOnPage < StoreConfig::num_objects_t(~0) && "Increase StoreConfig::num_objects_t size");
            VariableSizeObjectStore::BlockStorage storage = VariableSizeObjectStore::BlockStorage::init(
                    currentBlock, numObjectsOnPage, emptySpace);
            memcpy(&storage.offsets[0], &offsets[0], numObjectsOnPage * sizeof(StoreConfig::offset_t));
            memcpy(&storage.keys[0], &keys[0], numObjectsOnPage * sizeof(StoreConfig::key_t));
            numObjectsOnPage = 0;
            blocksGenerated++;
            currentBlock += StoreConfig::BLOCK_LENGTH;
            blockWritingPosition = 0;
            spaceLeftOnBlock = StoreConfig::BLOCK_LENGTH - VariableSizeObjectStore::overheadPerBlock;

            if (currentBlock >= buffer1 + BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH || forceFlush) {
                // Flush
                size_t generatedSinceLastFlush = blocksGenerated % BLOCK_FLUSH;
                if (generatedSinceLastFlush == 0) {
                    generatedSinceLastFlush = BLOCK_FLUSH;
                }
                size_t writeOffset = (blocksGenerated - generatedSinceLastFlush) * StoreConfig::BLOCK_LENGTH;
                if (writeOffset != 0) {
                    ioManager.awaitAny();
                }
                std::swap(buffer1, buffer2);
                ioManager.enqueueWrite(buffer2, writeOffset, generatedSinceLastFlush * StoreConfig::BLOCK_LENGTH, 1);
                ioManager.submit();
                currentBlock = buffer1;
            }
        }

        void close(uint16_t type) {
            if (spaceLeftOnBlock <= 128) {
                writeTable(true, spaceLeftOnBlock);
            } else {
                // Needs a terminator for the very last element
                write(0, 0, nullptr);
                writeTable(true, 42);
            }
            int result = ftruncate(fd, blocksGenerated * StoreConfig::BLOCK_LENGTH);
            (void) result;
            ioManager.awaitAny();

            result = pread(fd, buffer1, StoreConfig::BLOCK_LENGTH, 0);
            assert(result == StoreConfig::BLOCK_LENGTH);
            VariableSizeObjectStore::BlockStorage firstBlock(buffer1);
            assert(firstBlock.numObjects != 0);
            VariableSizeObjectStore::StoreMetadata metadata;
            metadata.numBlocks = blocksGenerated;
            metadata.maxSize = maxSize;
            metadata.type = type;
            memcpy(firstBlock.blockStart, &metadata, sizeof(VariableSizeObjectStore::StoreMetadata));
            result = pwrite(fd, buffer1, StoreConfig::BLOCK_LENGTH, 0);
            assert(result == StoreConfig::BLOCK_LENGTH);
            ::close(fd);
        }
};

} // Namespace pachash
