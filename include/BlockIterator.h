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
        IoManager &manager;
        size_t currentBucket = -1;
        char *currentContent = nullptr;
        size_t depth;
        char *buffer;
        size_t nextToSubmit = 0;
        size_t unusedBuffers = 0;
        std::vector<size_t> unusedBufferIndices;
        size_t maxBlocks;
    public:
        AnyBlockIterator(IoManager &ioManager, size_t depth, size_t maxBlocks)
                : manager(ioManager), depth(depth), maxBlocks(maxBlocks) {
            buffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, depth * PageConfig::PAGE_SIZE));
            unusedBufferIndices.resize(depth);
            for (int i = 0; i < depth; i++) {
                unusedBufferIndices.at(i) = i;
                unusedBuffers++;
            }
            next();
        }

        ~AnyBlockIterator() {
            free(buffer);
        }

        [[nodiscard]] size_t bucketNumber() const {
            return currentBucket;
        }

        [[nodiscard]] char *bucketContent() const {
            return currentContent;
        }

        void next() {
            if (currentContent != nullptr) {
                unusedBufferIndices.at(unusedBuffers) = (currentContent - buffer) / PageConfig::PAGE_SIZE;
                unusedBuffers++;
            }
            uint64_t peeked = manager.peekAny();
            if (peeked == 0) {
                while (unusedBuffers != 0 && nextToSubmit < maxBlocks) {
                    unusedBuffers--;
                    size_t bufferIdx = unusedBufferIndices.at(unusedBuffers);
                    uint64_t name = (bufferIdx << 32) | nextToSubmit;
                    manager.enqueueRead(buffer + bufferIdx*PageConfig::PAGE_SIZE, nextToSubmit*PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, name);
                    nextToSubmit++;
                }
                manager.submit();
                peeked = manager.awaitAny();
            }
            currentBucket = peeked & 0xffffffff;
            size_t bufferIdx = peeked >> 32;
            assert(currentBucket < nextToSubmit);
            assert(currentBucket < maxBlocks);
            assert(bufferIdx < depth);
            currentContent = buffer + bufferIdx * PageConfig::PAGE_SIZE;
        }
};
