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
 * The blocks are NOT guaranteed to arrive in order.
 */
class AnyBlockIterator {
    private:
        UringIO manager;
        size_t currentBucket = -1;
        char *currentContent = nullptr;
        size_t depth;
        char *buffer;
        size_t maxBlocks;
        std::vector<std::pair<size_t, size_t>> ranges;
    public:
        AnyBlockIterator(const char *filename, size_t depth, size_t maxBlocks)
                : manager(filename, O_DIRECT,  depth), depth(depth), maxBlocks(maxBlocks) {
            buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, depth * PageConfig::PAGE_SIZE));

            assert(maxBlocks > 3 * depth);
            ranges.resize(3 * depth);
            size_t blocksPerRange = maxBlocks/ranges.size();
            assert(blocksPerRange > 1);
            size_t block = 0;
            for (size_t i = 0; i < ranges.size(); i++) {
                if (i == ranges.size() - 1) {
                    ranges.at(i) = std::make_pair(block, maxBlocks);
                } else {
                    ranges.at(i) = std::make_pair(block, block + blocksPerRange);
                }
                block += blocksPerRange;
            }

            for (size_t i = 0; i < depth; i++) {
                size_t block = nextBlockToSubmit();
                uint64_t name = (i << 32) | block;
                manager.enqueueRead(buffer + i*PageConfig::PAGE_SIZE, block * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, name);
            }
            next();
        }

        ~AnyBlockIterator() {
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
