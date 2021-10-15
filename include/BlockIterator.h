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
        size_t currentBucketNumber = 0;
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

        [[nodiscard]] size_t bucketNumber() const {
            return currentBucketNumber;
        }

        [[nodiscard]] char *bucketContent() const {
            return file + currentBucketNumber * PageConfig::PAGE_SIZE;
        }

        void next() {
            currentBucketNumber++;
        }
};

/**
 * Iterates over an object store file and returns a pointer to each block.
 */
class PosixBlockIterator {
    private:
        int fd;
        size_t currentBucketNumber = -1;
        char *buffer;
        int batchSize;
    public:
        PosixBlockIterator(const char *filename, int batchSize) : batchSize(batchSize) {
            fd = open(filename, O_RDONLY | O_DIRECT);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, batchSize * PageConfig::PAGE_SIZE));
            next(); // Load first block
        }

        ~PosixBlockIterator() {
            close(fd);
            free(buffer);
        }

        [[nodiscard]] size_t bucketNumber() const {
            return currentBucketNumber;
        }

        [[nodiscard]] char *bucketContent() const {
            return buffer + (currentBucketNumber % batchSize) * PageConfig::PAGE_SIZE;
        }

        void next() {
            currentBucketNumber++;
            if (currentBucketNumber % batchSize == 0) {
                int read = pread(fd, buffer, batchSize * PageConfig::PAGE_SIZE, currentBucketNumber*PageConfig::PAGE_SIZE);
                if (read < PageConfig::PAGE_SIZE) {
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
        size_t currentBucket = -1;
        char *currentContent = nullptr;
        size_t depth;
        char *buffer;
        size_t maxBlocks;
        std::vector<std::pair<size_t, size_t>> ranges;
    public:
        UringAnyBlockIterator(const char *filename, size_t depth, size_t maxBlocks, bool randomize)
                : manager(filename, O_DIRECT,  depth), depth(depth), maxBlocks(maxBlocks) {
            buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, depth * PageConfig::PAGE_SIZE));

            if (randomize && maxBlocks > 3 * depth) {
                ranges.resize(3 * depth);
            } else {
                ranges.resize(1); // Scans linearly
            }
            size_t blocksPerRange = maxBlocks/ranges.size();
            assert(blocksPerRange > 0 || ranges.size() == 1);
            size_t block = 0;
            for (size_t i = 0; i < ranges.size(); i++) {
                if (i == ranges.size() - 1) {
                    ranges.at(i) = std::make_pair(block, maxBlocks);
                } else {
                    ranges.at(i) = std::make_pair(block, block + blocksPerRange);
                }
                block += blocksPerRange;
            }

            for (size_t i = 0; i < depth && i < maxBlocks; i++) {
                size_t block = nextBlockToSubmit();
                uint64_t name = (i << 32) | block;
                manager.enqueueRead(buffer + i*PageConfig::PAGE_SIZE, block * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, name);
            }
            next();
        }

        ~UringAnyBlockIterator() {
            free(buffer);
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

        [[nodiscard]] size_t bucketNumber() const {
            return currentBucket;
        }

        [[nodiscard]] char *bucketContent() const {
            return currentContent;
        }

        void next() {
            uint64_t peeked = manager.peekAny();
            if (peeked == 0) {
                manager.submit();
                peeked = manager.awaitAny();
            }
            currentBucket = peeked & 0xffffffff;
            size_t bufferIdx = peeked >> 32;
            assert(currentBucket < maxBlocks);
            assert(bufferIdx < depth);
            currentContent = buffer + bufferIdx * PageConfig::PAGE_SIZE;

            size_t block = nextBlockToSubmit();
            if (block < maxBlocks) {
                // Just enqueue, do not submit yet.
                uint64_t name = (bufferIdx << 32) | block;
                manager.enqueueRead(buffer + bufferIdx * PageConfig::PAGE_SIZE,
                                    block * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, name);
            }
        }
};

/**
 * Iterates over an object store file and returns a pointer to each block.
 */
class UringDoubleBufferBlockIterator {
    private:
        UringIO manager;
        size_t currentBucket = 0;
        char *currentContent1 = nullptr;
        char *currentContent2 = nullptr;
        size_t maxBlocks;
        int batchSize;
    public:
        UringDoubleBufferBlockIterator(const char *filename, size_t maxBlocks, int batchSize)
                : manager(filename, O_DIRECT, 1), batchSize(batchSize), maxBlocks(maxBlocks) {
            currentContent1 = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, batchSize * PageConfig::PAGE_SIZE));
            currentContent2 = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, batchSize * PageConfig::PAGE_SIZE));

            size_t toSubmit = std::min((size_t)batchSize, std::min(maxBlocks, maxBlocks - batchSize));
            manager.enqueueRead(currentContent1, currentBucket * PageConfig::PAGE_SIZE, toSubmit*PageConfig::PAGE_SIZE, 0);
            manager.submit();
            manager.awaitAny();
            if (toSubmit == batchSize) {
                toSubmit = std::min((size_t)batchSize, maxBlocks - batchSize);
                manager.enqueueRead(currentContent2, (currentBucket + batchSize) * PageConfig::PAGE_SIZE, toSubmit*PageConfig::PAGE_SIZE, 0);
                manager.submit();
            }
        }

        ~UringDoubleBufferBlockIterator() {
            free(currentContent1);
            free(currentContent2);
        }

        [[nodiscard]] size_t bucketNumber() const {
            return currentBucket;
        }

        [[nodiscard]] char *bucketContent() const {
            return currentContent1 + (currentBucket%batchSize) * PageConfig::PAGE_SIZE;
        }

        void next() {
            currentBucket++;
            if (currentBucket % batchSize == 0) {
                manager.awaitAny();
                std::swap(currentContent1, currentContent2);
                if (currentBucket < maxBlocks) {
                    size_t toSubmit = std::min((size_t)batchSize, maxBlocks - currentBucket - batchSize);
                    manager.enqueueRead(currentContent2, (currentBucket + batchSize) * PageConfig::PAGE_SIZE,
                                        toSubmit*PageConfig::PAGE_SIZE, 0);
                    manager.submit();
                }
            }
        }
};
