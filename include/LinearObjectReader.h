#pragma once

namespace pacthash {
template <bool reconstructObjects>
class LinearObjectReader {
    public:
        size_t numBlocks = 0;
        size_t currentBlock = 0;
        size_t maxSize = 0;
        char* currentElementPointer = nullptr;
        uint64_t currentKey = 0;
        size_t currentLength = 0;
    private:
        size_t currentElement = 0;
        UringDoubleBufferBlockIterator blockIterator;
        VariableSizeObjectStore::BlockStorage block;
        char* objectReconstructionBuffer = nullptr;
    public:
        bool completed = false;
        explicit LinearObjectReader(const char *filename, int flags)
                : numBlocks(VariableSizeObjectStore::readMetadata(filename).numBlocks),
                maxSize(VariableSizeObjectStore::readMetadata(filename).maxSize),
                blockIterator(UringDoubleBufferBlockIterator(filename, numBlocks, 250, flags)) {
            objectReconstructionBuffer = new char[maxSize];
            block = VariableSizeObjectStore::BlockStorage(blockIterator.blockContent());
            next(); // Skip pseudo object 0
        }

        ~LinearObjectReader() {
            delete[] objectReconstructionBuffer;
        }

        [[nodiscard]] bool hasEnded() const {
            return currentBlock >= numBlocks - 1;
        }

        void next() {
            assert(!hasEnded());
            currentElement++;
            currentKey = block.keys[currentElement];
            if (currentElement < size_t(block.numObjects - 1)) {
                // Object does not overlap. We already have the size
                // and the pointer does not need reconstruction. All is nice and easy.
                currentLength = block.offsets[currentElement + 1] - block.offsets[currentElement];
                currentElementPointer = block.blockStart + block.offsets[currentElement];
                return;
            } else {
                // Object overlaps. We need to find the size by examining the following blocks.
                // Also, we need to reconstruct the object to remove the headers in the middle.
                assert(currentElement == size_t(block.numObjects - 1));
                currentElementPointer = objectReconstructionBuffer;
                currentLength = block.tableStart - block.blockStart - block.offsets[currentElement] - block.emptyPageEnd;
                if (currentKey == 0) {
                    return;
                }
                assert(currentLength <= maxSize);
                if constexpr (reconstructObjects) {
                    memcpy(currentElementPointer, block.blockStart + block.offsets[currentElement], currentLength);
                }

                if (currentBlock == numBlocks - 1) {
                    currentBlock = ~0ul; // Done
                    return;
                }
                while (currentBlock < numBlocks - 1) {
                    nextBlock();
                    if (block.numObjects > 0) {
                        // We found the next object and therefore the end of this one.
                        StoreConfig::offset_t lengthOnNextBlock = block.offsets[0];
                        if constexpr (reconstructObjects) {
                            memcpy(currentElementPointer + currentLength, block.blockStart, lengthOnNextBlock);
                        }
                        currentLength += lengthOnNextBlock;
                        return;
                    } else {
                        // Fully overlapped. We have to copy the whole block and continue searching.
                        size_t lengthOnNextBlock = block.tableStart - block.blockStart;
                        if constexpr (reconstructObjects) {
                            memcpy(currentElementPointer + currentLength, block.blockStart, lengthOnNextBlock);
                        }
                        currentLength += lengthOnNextBlock - block.emptyPageEnd;
                    }
                }
                return;
            }
        }
    private:
        void nextBlock() {
            currentBlock++;
            blockIterator.next();
            block = VariableSizeObjectStore::BlockStorage(blockIterator.blockContent());
            currentElement = -1;
        }
};

} // Namespace pacthash
