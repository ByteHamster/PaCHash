#pragma once

class LinearObjectWriter {
    private:
        char *output = nullptr;
        size_t offset = 0;
        int fd;
        size_t mappedSize = 0;
        size_t numObjectsOnPage = 0;
        std::array<uint64_t, PageConfig::PAGE_SIZE/VariableSizeObjectStore::overheadPerObject> keys;
        std::array<uint16_t, PageConfig::PAGE_SIZE/VariableSizeObjectStore::overheadPerObject> lengths;
        size_t spaceLeftOnPage = PageConfig::PAGE_SIZE - VariableSizeObjectStore::overheadPerPage;
        size_t pageWritingPosition = 0;
        char *currentPage = nullptr;
        const char *filename = nullptr;
    public:
        size_t bucketsGenerated = 0;
        LinearObjectWriter(const char *filename, size_t initialBlocks) : filename(filename) {
            fd = open(filename, O_RDWR | O_CREAT, 0600);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            remap(initialBlocks);
            VariableSizeObjectStore::MetadataObjectType zero = 0;
            write(0, sizeof(VariableSizeObjectStore::MetadataObjectType), reinterpret_cast<const char *>(&zero));
        }

        void remap(size_t blocks) {
            if (output != nullptr) {
                munmap(output, mappedSize);
            }
            mappedSize = blocks * PageConfig::PAGE_SIZE;
            ftruncate(fd, mappedSize);
            output = static_cast<char *>(mmap(nullptr, mappedSize, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0));
            assert(output != MAP_FAILED);
            currentPage = output + bucketsGenerated * PageConfig::PAGE_SIZE;
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
                    writeTable();
                }
            }
        }

        void writeTable() {
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

            if (bucketsGenerated * PageConfig::PAGE_SIZE >= mappedSize) {
                remap(bucketsGenerated * 1.5 + 1);
            }
        }

        void close() {
            writeTable();
            VariableSizeObjectStore::BlockStorage firstBlock(output);
            firstBlock.calculateObjectPositions();
            VariableSizeObjectStore::MetadataObjectType metadata = bucketsGenerated;
            memcpy(firstBlock.objects[0], &metadata, sizeof(VariableSizeObjectStore::MetadataObjectType));
            munmap(output, mappedSize);
            ftruncate(fd, (off_t)(bucketsGenerated + 1) * PageConfig::PAGE_SIZE);
            ::close(fd);
        }
};
