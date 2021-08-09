#ifndef TESTCOMPARISON_PAGEDFILEOUTPUTSTREAM_H
#define TESTCOMPARISON_PAGEDFILEOUTPUTSTREAM_H

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdio>
#include <fcntl.h>

#include "PageConfig.h"

/**
 * Page-aware file output stream. The first 2 bytes of each page store an offset
 * of the first element on the page.
 */
class PagedFileOutputStream {
    private:
        int fd;
        char *file = nullptr;
        size_t mappedSize = 0;
        size_t position = 0;
        size_t blockOffsetWritten = -1;
    public:

        PagedFileOutputStream(std::string filename, size_t estimatedSize) {
            fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
            assert(fd >= 0);
            remap(estimatedSize);
        }

        void close() {
            msync(file, mappedSize, MS_SYNC);
            munmap(file, mappedSize);
            fsync(fd);
            ftruncate(fd, position);
        }

        void remap(size_t size) {
            msync(file, mappedSize, MS_SYNC);
            munmap(file, mappedSize);
            ftruncate(fd, size);
            file = static_cast<char *>(mmap(nullptr, size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0));
            assert(file != MAP_FAILED);
            mappedSize = size;
        }

        void write(const char *data, size_t n) {
            assert(n > 0);
            if (position + n + n/PageConfig::PAGE_SIZE + 1 >= mappedSize) {
                remap(1.1 * mappedSize);
            }
            size_t copied = 0;
            do {
                if (position % PageConfig::PAGE_SIZE == 0) {
                    // Start of a page. Leave space for offset indicator
                    *reinterpret_cast<PageConfig::offset_t *>(&file[position]) = 0;
                    position += sizeof(PageConfig::offset_t);
                }
                size_t spaceRemainingOnPage = PageConfig::PAGE_SIZE - (position % PageConfig::PAGE_SIZE);
                size_t writeThisTime = std::min(n - copied, spaceRemainingOnPage);

                memcpy(&file[position], &data[copied], writeThisTime);
                position += writeThisTime;
                copied += writeThisTime;
            } while (copied < n);
        }

        template<class T>
        void write(T object) {
            write(reinterpret_cast<const char *>(&object), sizeof(object));
        }

        void notifyObjectStart() {
            size_t block = position / PageConfig::PAGE_SIZE;
            if (block != blockOffsetWritten) {
                blockOffsetWritten = block;
                size_t blockStart = block * PageConfig::PAGE_SIZE;
                PageConfig::offset_t offset = position - blockStart;
                *reinterpret_cast<PageConfig::offset_t *>(&file[blockStart]) = offset;
            }
        }

        /**
         * The position is the actual position on the page and can overwrite the offsets.
         */
        void writeAt(size_t position, const char *data, size_t n) {
            memcpy(&file[position], data, n);
        }

        template<class T>
        void writeAt(size_t position, T object) {
            writeAt(position, reinterpret_cast<const char *>(&object), sizeof(object));
        }
};


#endif //TESTCOMPARISON_PAGEDFILEOUTPUTSTREAM_H
