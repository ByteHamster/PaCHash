#pragma once

class LinearObjectWriter {
    private:
        static constexpr size_t BLOCK_FLUSH = 250;
        size_t offset = 0;
        int fd;
        size_t numObjectsOnPage = 0;
        std::array<uint64_t, PageConfig::PAGE_SIZE/VariableSizeObjectStore::overheadPerObject> keys;
        std::array<uint16_t, PageConfig::PAGE_SIZE/VariableSizeObjectStore::overheadPerObject> lengths;
        size_t spaceLeftOnPage = PageConfig::PAGE_SIZE - VariableSizeObjectStore::overheadPerPage;
        size_t pageWritingPosition = 0;
        char *currentPage = nullptr;
        char *buffer = nullptr;
        const char *filename = nullptr;
    public:
        size_t bucketsGenerated = 0;
        explicit LinearObjectWriter(const char *filename) : filename(filename) {
            fd = open(filename, O_RDWR | O_CREAT | O_DIRECT, 0600);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, BLOCK_FLUSH * PageConfig::PAGE_SIZE));
            currentPage = buffer;
            VariableSizeObjectStore::MetadataObjectType zero = 0;
            write(0, sizeof(VariableSizeObjectStore::MetadataObjectType), reinterpret_cast<const char *>(&zero));
        }

        void write(uint64_t key, size_t length, const char* content) {
            size_t written = 0;
            keys[numObjectsOnPage] = key;
            lengths[numObjectsOnPage] = length;
            numObjectsOnPage++;
            spaceLeftOnPage -= VariableSizeObjectStore::overheadPerObject;

            while (written < length) {
                size_t toWrite = std::min(spaceLeftOnPage, length - written);
                memcpy(currentPage + pageWritingPosition, content + written, toWrite);
                if (pageWritingPosition == 0 && written != 0) {
                    offset = toWrite;
                }
                pageWritingPosition += toWrite;
                spaceLeftOnPage -= toWrite;
                written += toWrite;

                if (spaceLeftOnPage <= VariableSizeObjectStore::overheadPerObject) {
                    // No more object fits on the page.
                    writeTable(false);
                }
            }
        }

        void writeTable(bool forceFlush) {
            assert(pageWritingPosition <= PageConfig::PAGE_SIZE);
            VariableSizeObjectStore::BlockStorage storage = VariableSizeObjectStore::BlockStorage::init(currentPage, offset, numObjectsOnPage);
            for (int i = 0; i < numObjectsOnPage; i++) {
                storage.lengths[i] = lengths[i];
                storage.keys[i] = keys[i];
            }
            numObjectsOnPage = 0;
            bucketsGenerated++;
            currentPage += PageConfig::PAGE_SIZE;
            pageWritingPosition = 0;
            spaceLeftOnPage = PageConfig::PAGE_SIZE - VariableSizeObjectStore::overheadPerPage;
            offset = 0;

            if (currentPage >= buffer + BLOCK_FLUSH * PageConfig::PAGE_SIZE || forceFlush) {
                // Flush
                currentPage = buffer;
                int result = ftruncate(fd, bucketsGenerated * PageConfig::PAGE_SIZE);
                (void) result;
                size_t generatedSinceLastFlush = bucketsGenerated % BLOCK_FLUSH;
                if (generatedSinceLastFlush == 0) {
                    generatedSinceLastFlush = BLOCK_FLUSH;
                }
                result = pwrite(fd, buffer, generatedSinceLastFlush * PageConfig::PAGE_SIZE, (bucketsGenerated - generatedSinceLastFlush) * PageConfig::PAGE_SIZE);
                assert(result == generatedSinceLastFlush * PageConfig::PAGE_SIZE);
            }
        }

        void close() {
            writeTable(true);

            pread(fd, buffer, PageConfig::PAGE_SIZE, 0);
            VariableSizeObjectStore::BlockStorage firstBlock(buffer);
            firstBlock.calculateObjectPositions();
            assert(firstBlock.numObjects != 0);
            VariableSizeObjectStore::MetadataObjectType metadata = bucketsGenerated;
            memcpy(firstBlock.objects[0], &metadata, sizeof(VariableSizeObjectStore::MetadataObjectType));
            pwrite(fd, buffer, PageConfig::PAGE_SIZE, 0);
            ::close(fd);
        }
};
