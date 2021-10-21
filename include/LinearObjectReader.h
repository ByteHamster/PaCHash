#pragma once

class LinearObjectReader {
    public:
        size_t numBlocks;
    private:
        size_t currentBlock = 0;
        size_t currentElement = 0;
        UringDoubleBufferBlockIterator blockIterator;
        VariableSizeObjectStore::BlockStorage *block = nullptr;
        char* objectReconstructionBuffer = nullptr;
    public:
        bool completed = false;
        explicit LinearObjectReader(const char *filename, int flags)
                : numBlocks(EliasFanoObjectStore<8>::readSpecialObject0(filename)),
                  blockIterator(filename, numBlocks, 250, flags) {
            objectReconstructionBuffer = new char[StoreConfig::MAX_OBJECT_SIZE];
            block = new VariableSizeObjectStore::BlockStorage(blockIterator.blockContent());
            block->calculateObjectPositions();
            next(); // Skip pseudo object 0
        }

        ~LinearObjectReader() {
            delete block;
            block = nullptr;
            delete[] objectReconstructionBuffer;
        }

        [[nodiscard]] bool hasEnded() const {
            return currentBlock >= numBlocks;
        }

        void next() {
            if (currentElement == -1) {
                currentElement++; // Already loaded new block (overlapping) but did not increment object yet
                return;
            }
            assert(!hasEnded());
            if (block != nullptr && currentElement + 1 < block->numObjects) {
                currentElement++;
            } else {
                if (currentBlock + 1 >= numBlocks) {
                    currentBlock++;
                    return;
                }
                currentElement = 0;
                do {
                    nextBlock();
                } while (block->numObjects == 0 && currentBlock < numBlocks - 1);
            }
        }

        void nextBlock() {
            delete block;
            currentBlock++;
            blockIterator.next();
            block = new VariableSizeObjectStore::BlockStorage(blockIterator.blockContent());
            block->calculateObjectPositions();
            if (currentBlock == numBlocks - 1 && block->numObjects == 0) {
                currentBlock++; // Indicator for "ended"
            }
        }

        StoreConfig::key_t currentKey() {
            assert(currentElement < block->numObjects);
            return block->keys[currentElement];
        }

        StoreConfig::length_t currentLength() {
            assert(currentElement < block->numObjects);
            return block->lengths[currentElement];
        }

        /**
         * Might load the next block, so after calling this function, the currentX() methods cannot be used
         * without calling next().
         */
        char *currentContent() {
            assert(currentElement < block->numObjects);
            StoreConfig::length_t length =  block->lengths[currentElement];
            char *pointer = block->objects[currentElement];
            size_t spaceLeft = block->tableStart - block->objects[currentElement];
            if (spaceLeft >= length) {
                // No copying needed
                return pointer;
            }

            memcpy(objectReconstructionBuffer, pointer, spaceLeft);
            StoreConfig::length_t reconstructed = spaceLeft;
            char *readTo = objectReconstructionBuffer + spaceLeft;
            while (reconstructed < length) {
                nextBlock();
                currentElement = -1;
                StoreConfig::length_t spaceInNextBucket = (block->tableStart - block->blockStart);
                assert(spaceInNextBucket <= StoreConfig::BLOCK_LENGTH);
                StoreConfig::length_t spaceToCopy = std::min(static_cast<StoreConfig::length_t>(length - reconstructed), spaceInNextBucket);
                assert(spaceToCopy > 0 && spaceToCopy <= StoreConfig::MAX_OBJECT_SIZE);
                memcpy(readTo, block->blockStart, spaceToCopy);
                reconstructed += spaceToCopy;
                readTo += spaceToCopy;
                assert(reconstructed <= StoreConfig::MAX_OBJECT_SIZE);
            }
            return objectReconstructionBuffer;
        }
};
