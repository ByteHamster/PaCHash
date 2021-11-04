#pragma once

/**
 * Iterates over an object store file and returns a pointer to each block.
 * The blocks are guaranteed to arrive in order.
 */
class MemoryMapBlockIterator {
    private:
        int fd;
        char *file;
        size_t fileSize;
        size_t currentBlockNumber = 0;
    public:
        MemoryMapBlockIterator(const char *filename, size_t fileSize) : fileSize(fileSize) {
            fd = open(filename, O_RDONLY);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
            madvise(file, fileSize, MADV_SEQUENTIAL | MADV_WILLNEED | MADV_HUGEPAGE);
        }

        ~MemoryMapBlockIterator() {
            munmap(file, fileSize);
            close(fd);
        }

        [[nodiscard]] size_t blockNumber() const {
            return currentBlockNumber;
        }

        [[nodiscard]] char *blockContent() const {
            return file + currentBlockNumber * StoreConfig::BLOCK_LENGTH;
        }

        void next() {
            currentBlockNumber++;
        }
};

/**
 * Iterates over an object store file and returns a pointer to each block.
 */
class PosixBlockIterator {
    private:
        int fd;
        size_t currentBlockNumber = -1;
        char *buffer;
        size_t batchSize;
    public:
        PosixBlockIterator(const char *filename, size_t batchSize, int flags) : batchSize(batchSize) {
            fd = open(filename, O_RDONLY | flags);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[batchSize * StoreConfig::BLOCK_LENGTH];
            next(); // Load first block
        }

        ~PosixBlockIterator() {
            close(fd);
            delete[] buffer;
        }

        [[nodiscard]] size_t blockNumber() const {
            return currentBlockNumber;
        }

        [[nodiscard]] char *blockContent() const {
            return buffer + (currentBlockNumber % batchSize) * StoreConfig::BLOCK_LENGTH;
        }

        void next() {
            currentBlockNumber++;
            if (currentBlockNumber % batchSize == 0) {
                uint read = pread(fd, buffer, batchSize * StoreConfig::BLOCK_LENGTH, currentBlockNumber * StoreConfig::BLOCK_LENGTH);
                if (read < StoreConfig::BLOCK_LENGTH) {
                    std::cerr<<"Read not enough"<<std::endl;
                    exit(1);
                }
            }
        }
};

/**
 * Iterates over an object store file and returns a pointer to each block.
 * The blocks are NOT guaranteed to arrive in order.
 */
class UringAnyBlockIterator {
    private:
        UringIO manager;
        size_t currentBlock = -1;
        char *currentContent = nullptr;
        size_t depth;
        char *buffer;
        size_t maxBlocks;
        std::vector<std::pair<size_t, size_t>> ranges;
    public:
        UringAnyBlockIterator(const char *filename, size_t depth, size_t maxBlocks, bool randomize, int flags)
                : manager(filename, flags,  depth), depth(depth), maxBlocks(maxBlocks) {
            buffer = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[depth * StoreConfig::BLOCK_LENGTH];

            if (randomize && maxBlocks > 3 * depth) {
                ranges.resize(3 * depth);
            } else {
                ranges.resize(1); // Scans linearly
            }
            size_t blocksPerRange = maxBlocks/ranges.size();
            assert(blocksPerRange > 0 || ranges.size() == 1);
            size_t rangeStart = 0;
            for (size_t i = 0; i < ranges.size(); i++) {
                if (i == ranges.size() - 1) {
                    ranges.at(i) = std::make_pair(rangeStart, maxBlocks);
                } else {
                    ranges.at(i) = std::make_pair(rangeStart, rangeStart + blocksPerRange);
                }
                rangeStart += blocksPerRange;
            }

            for (size_t i = 0; i < depth && i < maxBlocks; i++) {
                size_t block = nextBlockToSubmit();
                uint64_t name = (i << 32) | block;
                manager.enqueueRead(buffer + i * StoreConfig::BLOCK_LENGTH, block * StoreConfig::BLOCK_LENGTH, StoreConfig::BLOCK_LENGTH, name);
            }
            next();
        }

        ~UringAnyBlockIterator() {
            delete[] buffer;
        }

        size_t nextBlockToSubmit() {
            size_t size = ranges.size();
            if (size == 0) {
                return maxBlocks;
            }
            size_t range = rand() % size;
            size_t nextBlock = ranges[range].first;
            ranges[range].first++;
            if (ranges[range].first == ranges[range].second) {
                ranges.erase(ranges.begin() + range);
            }
            return nextBlock;
        }

        [[nodiscard]] size_t blockNumber() const {
            return currentBlock;
        }

        [[nodiscard]] char *blockContent() const {
            return currentContent;
        }

        void next() {
            uint64_t peeked = manager.peekAny();
            if (peeked == 0) {
                manager.submit();
                peeked = manager.awaitAny();
            }
            currentBlock = peeked & 0xffffffff;
            size_t bufferIdx = peeked >> 32;
            assert(currentBlock < maxBlocks);
            assert(bufferIdx < depth);
            currentContent = buffer + bufferIdx * StoreConfig::BLOCK_LENGTH;

            size_t block = nextBlockToSubmit();
            if (block < maxBlocks) {
                // Just enqueue, do not submit yet.
                uint64_t name = (bufferIdx << 32) | block;
                manager.enqueueRead(buffer + bufferIdx * StoreConfig::BLOCK_LENGTH,
                                    block * StoreConfig::BLOCK_LENGTH, StoreConfig::BLOCK_LENGTH, name);
            }
        }
};

/**
 * Iterates over an object store file and returns a pointer to each block.
 */
class UringDoubleBufferBlockIterator {
    private:
        UringIO manager;
        size_t currentBlock = 0;
        char *currentContent1 = nullptr;
        char *currentContent2 = nullptr;
        size_t maxBlocks;
        size_t batchSize;
    public:
        UringDoubleBufferBlockIterator(const char *filename, size_t maxBlocks, size_t batchSize, int flags)
                : manager(filename, flags, 1), maxBlocks(maxBlocks), batchSize(batchSize) {
            currentContent1 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[batchSize * StoreConfig::BLOCK_LENGTH];
            currentContent2 = new (std::align_val_t(StoreConfig::BLOCK_LENGTH)) char[batchSize * StoreConfig::BLOCK_LENGTH];

            size_t toSubmit = std::min(batchSize, maxBlocks);
            manager.enqueueRead(currentContent1, currentBlock * StoreConfig::BLOCK_LENGTH, toSubmit * StoreConfig::BLOCK_LENGTH, 0);
            manager.submit();
            manager.awaitAny();
            if (toSubmit == batchSize) {
                toSubmit = std::min((size_t)batchSize, maxBlocks - batchSize);
                manager.enqueueRead(currentContent2, (currentBlock + batchSize) * StoreConfig::BLOCK_LENGTH, toSubmit * StoreConfig::BLOCK_LENGTH, 0);
                manager.submit();
            }
        }

        ~UringDoubleBufferBlockIterator() {
            delete[] currentContent1;
            delete[] currentContent2;
        }

        [[nodiscard]] size_t blockNumber() const {
            return currentBlock;
        }

        [[nodiscard]] char *blockContent() const {
            return currentContent1 + (currentBlock % batchSize) * StoreConfig::BLOCK_LENGTH;
        }

        void next() {
            currentBlock++;
            assert(currentBlock < maxBlocks);
            if (currentBlock % batchSize == 0) {
                manager.awaitAny();
                std::swap(currentContent1, currentContent2);
                if (currentBlock < maxBlocks) {
                    size_t toSubmit = std::min((size_t)batchSize, maxBlocks - currentBlock - batchSize);
                    manager.enqueueRead(currentContent2, (currentBlock + batchSize) * StoreConfig::BLOCK_LENGTH,
                                        toSubmit * StoreConfig::BLOCK_LENGTH, 0);
                    manager.submit();
                }
            }
        }
};
