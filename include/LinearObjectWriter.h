#pragma once

class SetObjectProvider : public ObjectProvider {
    private:
        std::array<VariableSizeObjectStore::Item, PageConfig::PAGE_SIZE> items;
        size_t size = 0;
        // Searching round-robin is fine because the items are inserted and requested in order
        size_t searchItemRoundRobin = 0;
    public:
        void insert(VariableSizeObjectStore::Item &item) {
            items.at(size) = item;
            size++;
        }

        void clear() {
            size = 0;
            searchItemRoundRobin = 0;
        }

        [[nodiscard]] size_t getLength(uint64_t key) final {
            while (items.at(searchItemRoundRobin).key != key) {
                searchItemRoundRobin = (searchItemRoundRobin + 1) % size;
            }
            return items.at(searchItemRoundRobin).length;
        }

        [[nodiscard]] const char *getValue(uint64_t key) final {
            while (items.at(searchItemRoundRobin).key != key) {
                searchItemRoundRobin = (searchItemRoundRobin + 1) % size;
            }
            return reinterpret_cast<const char *>(items.at(searchItemRoundRobin).userData);
        }
};

class LinearObjectWriter {
    private:
        char *output;
        size_t offset = 0;
        size_t fileSize;
        int fd;
        VariableSizeObjectStore::Bucket currentBucket;
        VariableSizeObjectStore::Bucket previousBucket;
        SetObjectProvider *currentObjectProvider;
        SetObjectProvider *previousObjectProvider;
    public:
        size_t bucketsGenerated = 0;
        LinearObjectWriter(const char *filename, size_t totalBlocks) {
            fd = open(filename, O_RDWR | O_CREAT, 0600);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }

            fileSize = (totalBlocks+1) * PageConfig::PAGE_SIZE;
            ftruncate(fd, fileSize);
            output = static_cast<char *>(mmap(nullptr, fileSize, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0));
            assert(output != MAP_FAILED);

            currentBucket.length = VariableSizeObjectStore::overheadPerPage;
            currentObjectProvider = new SetObjectProvider();
            previousObjectProvider = new SetObjectProvider();

            VariableSizeObjectStore::Item metadataItem {
                    0, sizeof(VariableSizeObjectStore::MetadataObjectType),
                    reinterpret_cast<uint64_t>(&offset)}; // Random content for now
            write(metadataItem);
        }

        ~LinearObjectWriter() {
            delete currentObjectProvider;
            delete previousObjectProvider;
        }

        void write(VariableSizeObjectStore::Item item) {
            currentBucket.length += VariableSizeObjectStore::overheadPerObject + item.length;
            currentBucket.items.push_back(item);
            currentObjectProvider->insert(item);

            // Flush
            if (currentBucket.length + VariableSizeObjectStore::overheadPerObject >= PageConfig::PAGE_SIZE) {
                if (bucketsGenerated > 0) {
                    auto storage = VariableSizeObjectStore::BlockStorage::init(
                            output + (bucketsGenerated-1)*PageConfig::PAGE_SIZE, offset, previousBucket.items.size());
                    storage.pageStart[PageConfig::PAGE_SIZE - 1] = 42;
                    offset = VariableSizeObjectStore::writeBucket(previousBucket, storage, *previousObjectProvider,
                                                                  true, currentBucket.items.size());
                }

                previousBucket = currentBucket;
                std::swap(previousObjectProvider, currentObjectProvider);
                currentBucket.items.clear();
                currentObjectProvider->clear();
                if (currentBucket.length > PageConfig::PAGE_SIZE) {
                    currentBucket.length -= PageConfig::PAGE_SIZE; // Overlap
                } else {
                    currentBucket.length = 0;
                }
                currentBucket.length += VariableSizeObjectStore::overheadPerPage;
                bucketsGenerated++;
            }
        }

        void flushEnd() {
            auto storage = VariableSizeObjectStore::BlockStorage::init(
                    output + (bucketsGenerated-1)*PageConfig::PAGE_SIZE, offset, previousBucket.items.size());
            offset = VariableSizeObjectStore::writeBucket(previousBucket, storage, *previousObjectProvider, true, currentBucket.items.size());

            auto storage2 = VariableSizeObjectStore::BlockStorage::init(
                    output + (bucketsGenerated)*PageConfig::PAGE_SIZE, offset, currentBucket.items.size());
            offset = VariableSizeObjectStore::writeBucket(currentBucket, storage2, *currentObjectProvider, true, 0);

            VariableSizeObjectStore::MetadataObjectType metadataNumBlocks = bucketsGenerated+1;
            VariableSizeObjectStore::BlockStorage firstBlock(output);
            memcpy(firstBlock.objectsStart, &metadataNumBlocks, sizeof(VariableSizeObjectStore::MetadataObjectType));
        }

        void close() {
            munmap(output, fileSize);
            ftruncate(fd, (off_t)(bucketsGenerated + 1) * PageConfig::PAGE_SIZE);
            ::close(fd);
        }
};
