#pragma once

class LinearObjectReader {
    public:
        size_t numBlocks;
    private:
        size_t currentBlock = 0;
        size_t currentElement = 0;
        char* currentElementInBlock;
        UringDoubleBufferBlockIterator blockIterator;
        VariableSizeObjectStore::BlockStorage block;
        char* objectReconstructionBuffer = nullptr;
    public:
        bool completed = false;
        explicit LinearObjectReader(const char *filename, int flags)
                : numBlocks(VariableSizeObjectStore::readSpecialObject0(filename)),
                  blockIterator(filename, numBlocks, 250, flags) {
            objectReconstructionBuffer = new char[StoreConfig::MAX_OBJECT_SIZE];
            block = VariableSizeObjectStore::BlockStorage(blockIterator.blockContent());
            currentElementInBlock = block.objectsStart;
            next(); // Skip pseudo object 0
        }

        ~LinearObjectReader() {
            delete[] objectReconstructionBuffer;
        }

        [[nodiscard]] bool hasEnded() const {
            return currentBlock >= numBlocks;
        }

        void next() {
            if (currentElement == ~0ul) {
                currentElementInBlock = block.objectsStart;
                currentElement++; // Already loaded new block (overlapping) but did not increment object yet
                return;
            }
            assert(!hasEnded());
            if (block.objectsStart != nullptr && currentElement + 1 < block.numObjects) {
                currentElementInBlock += block.lengths[currentElement];
                currentElement++;
            } else {
                if (currentBlock + 1 >= numBlocks) {
                    currentBlock++;
                    return;
                }
                currentElement = 0;
                do {
                    nextBlock();
                } while (block.numObjects == 0 && currentBlock < numBlocks - 1);
            }
        }

        void nextBlock() {
            currentBlock++;
            blockIterator.next();
            block = VariableSizeObjectStore::BlockStorage(blockIterator.blockContent());
            currentElementInBlock = block.objectsStart;
            if (currentBlock == numBlocks - 1 && block.numObjects == 0) {
                currentBlock++; // Indicator for "ended"
            }
        }

        [[nodiscard]] StoreConfig::key_t currentKey() const {
            assert(currentElement < block.numObjects);
            return block.keys[currentElement];
        }

        [[nodiscard]] StoreConfig::length_t currentLength() const {
            assert(currentElement < block.numObjects);
            return block.lengths[currentElement];
        }

        /**
         * Might load the next block, so after calling this function, the currentX() methods cannot be used
         * without calling next().
         */
        char *currentContent() {
            assert(currentElement < block.numObjects);
            StoreConfig::length_t length =  block.lengths[currentElement];
            char *pointer = currentElementInBlock;
            size_t spaceLeft = block.tableStart - pointer;
            if (spaceLeft >= length) {
                // No copying needed
                return pointer;
            }

            memcpy(objectReconstructionBuffer, pointer, spaceLeft);
            StoreConfig::length_t reconstructed = spaceLeft;
            char *readTo = objectReconstructionBuffer + spaceLeft;
            while (reconstructed < length) {
                nextBlock();
                currentElement = ~0ul;
                StoreConfig::length_t spaceInNextBucket = (block.tableStart - block.blockStart);
                assert(spaceInNextBucket <= StoreConfig::BLOCK_LENGTH);
                StoreConfig::length_t spaceToCopy = std::min(static_cast<StoreConfig::length_t>(length - reconstructed), spaceInNextBucket);
                assert(spaceToCopy > 0 && spaceToCopy <= StoreConfig::MAX_OBJECT_SIZE);
                memcpy(readTo, block.blockStart, spaceToCopy);
                reconstructed += spaceToCopy;
                readTo += spaceToCopy;
                assert(reconstructed <= StoreConfig::MAX_OBJECT_SIZE);
            }
            return objectReconstructionBuffer;
        }
};
