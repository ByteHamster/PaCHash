#pragma once

class LinearObjectWriter {
    private:
        static constexpr size_t BLOCK_FLUSH = 250;
        StoreConfig::length_t offset = 0;
        int fd;
        size_t numObjectsOnPage = 0;
        StoreConfig::key_t keys[StoreConfig::BLOCK_LENGTH / VariableSizeObjectStore::overheadPerObject] = {0};
        StoreConfig::length_t lengths[StoreConfig::BLOCK_LENGTH / VariableSizeObjectStore::overheadPerObject] = {0};
        StoreConfig::length_t spaceLeftOnBlock = StoreConfig::BLOCK_LENGTH - VariableSizeObjectStore::overheadPerBlock;
        size_t blockWritingPosition = 0;
        char *currentBlock = nullptr;
        char *buffer1 = nullptr;
        char *buffer2 = nullptr;
        size_t fileSizeBlocks = 0;
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
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            buffer1 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH];
            buffer2 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH];
            currentBlock = buffer1;
            VariableSizeObjectStore::MetadataObjectType zero = 0;
            write(0, sizeof(VariableSizeObjectStore::MetadataObjectType), reinterpret_cast<const char *>(&zero));
        }

        ~LinearObjectWriter() {
            delete[] buffer1;
            delete[] buffer2;
        }

        void write(StoreConfig::key_t key, StoreConfig::length_t length, const char* content) {
            StoreConfig::length_t written = 0;
            keys[numObjectsOnPage] = key;
            lengths[numObjectsOnPage] = length;
            numObjectsOnPage++;
            spaceLeftOnBlock -= VariableSizeObjectStore::overheadPerObject;

            do {
                StoreConfig::length_t toWrite = std::min(spaceLeftOnBlock, static_cast<StoreConfig::length_t>(length - written));
                memcpy(currentBlock + blockWritingPosition, content + written, toWrite);
                blockWritingPosition += toWrite;
                spaceLeftOnBlock -= toWrite;
                written += toWrite;

                if (spaceLeftOnBlock <= VariableSizeObjectStore::overheadPerObject) {
                    // No more object fits on the page.
                    writeTable(false);
                    offset = length - written;
                }
            } while (written < length);
        }

        void writeTable(bool forceFlush) {
            assert(blockWritingPosition <= StoreConfig::BLOCK_LENGTH);
            VariableSizeObjectStore::BlockStorage storage = VariableSizeObjectStore::BlockStorage::init(currentBlock, offset, numObjectsOnPage);
            memcpy(&storage.lengths[0], &lengths[0], numObjectsOnPage * sizeof(StoreConfig::length_t));
            memcpy(&storage.keys[0], &keys[0], numObjectsOnPage * sizeof(StoreConfig::key_t));
            numObjectsOnPage = 0;
            blocksGenerated++;
            currentBlock += StoreConfig::BLOCK_LENGTH;
            blockWritingPosition = 0;
            spaceLeftOnBlock = StoreConfig::BLOCK_LENGTH - VariableSizeObjectStore::overheadPerBlock;

            if (currentBlock >= buffer1 + BLOCK_FLUSH * StoreConfig::BLOCK_LENGTH || forceFlush) {
                // Flush
                if (blocksGenerated >= fileSizeBlocks) {
                    fileSizeBlocks = 1.5 * blocksGenerated;
                    int result = ftruncate(fd, fileSizeBlocks * StoreConfig::BLOCK_LENGTH);
                    (void) result;
                }
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

        void close() {
            writeTable(true);
            int result = ftruncate(fd, blocksGenerated * StoreConfig::BLOCK_LENGTH);
            (void) result;
            ioManager.awaitAny();

            result = pread(fd, buffer1, StoreConfig::BLOCK_LENGTH, 0);
            assert(result == StoreConfig::BLOCK_LENGTH);
            VariableSizeObjectStore::BlockStorage firstBlock(buffer1);
            assert(firstBlock.numObjects != 0);
            VariableSizeObjectStore::MetadataObjectType metadata = blocksGenerated;
            memcpy(firstBlock.objectsStart, &metadata, sizeof(VariableSizeObjectStore::MetadataObjectType));
            result = pwrite(fd, buffer1, StoreConfig::BLOCK_LENGTH, 0);
            assert(result == StoreConfig::BLOCK_LENGTH);
            ::close(fd);
        }
};
