#ifndef TESTCOMPARISON_PAGEDOBJECTRECONSTRUCTOR_H
#define TESTCOMPARISON_PAGEDOBJECTRECONSTRUCTOR_H

#include "PageConfig.h"

/**
 * Page-aware buffer reader. The first 2 bytes of each page store an offset
 * of the first element on the page. Therefore, when reading an object from the buffer,
 * these bytes need to be skipped.
 */
class PagedObjectReconstructor {
    public:
        size_t position = 0;
    private:
        char *file = nullptr;
        char *outputBuffer;
    public:

        explicit PagedObjectReconstructor(char *buffer, char *possibleOutputBuffer) {
            outputBuffer = possibleOutputBuffer;
            file = buffer;
            PageConfig::offset_t offset = *reinterpret_cast<PageConfig::offset_t *>(&file[0]);
            position = offset;
        }

        /**
         * Returns a pointer to the read data of size \p n. The pointer might either point to the
         * output buffer given to the constructor or directly to the input buffer, depending on the page boundaries.
         * The pointer is valid until the next call to the function.
         */
        char* read(size_t n) {
            assert(n > 0);
            assert(n <= PageConfig::MAX_OBJECT_SIZE);

            size_t spaceRemainingOnPage = PageConfig::PAGE_SIZE - (position % PageConfig::PAGE_SIZE);
            if (spaceRemainingOnPage > n && (position % PageConfig::PAGE_SIZE != 0)) {
                // No separators within item. No need to copy.
                position += n;
                return &file[position - n];
            }

            size_t readPosition = 0;
            do {
                if (position % PageConfig::PAGE_SIZE == 0) {
                    // Start of a page. Skip offset indicator
                    position += sizeof(PageConfig::offset_t);
                }
                size_t spaceRemainingOnPage = PageConfig::PAGE_SIZE - (position % PageConfig::PAGE_SIZE);
                size_t readThisTime = std::min(n - readPosition, spaceRemainingOnPage);

                memcpy(&outputBuffer[readPosition], &file[position], readThisTime);
                position += readThisTime;
                readPosition += readThisTime;
            } while (readPosition < n);
            return outputBuffer;
        }

        void skip(size_t n) {
            assert(n > 0);
            size_t toSkip = n;
            do {
                if (position % PageConfig::PAGE_SIZE == 0) {
                    // Start of a page. Skip offset indicator
                    position += sizeof(PageConfig::offset_t);
                }
                size_t spaceRemainingOnPage = PageConfig::PAGE_SIZE - (position % PageConfig::PAGE_SIZE);
                size_t skipThisTime = std::min(toSkip, spaceRemainingOnPage);
                position += skipThisTime;
                toSkip -= skipThisTime;
            } while (toSkip > 0);
        }

        template<class T>
        T* read() {
            return reinterpret_cast<T *>(read(sizeof(T)));
        }
};


#endif //TESTCOMPARISON_PAGEDOBJECTRECONSTRUCTOR_H
