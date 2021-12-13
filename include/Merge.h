#pragma once

#include "LinearObjectReader.h"

namespace pacthash {
void merge(std::vector<std::string> &inputFiles, std::string &outputFile) {
    std::vector<LinearObjectReader<true>> readers;
    readers.reserve(inputFiles.size());
    size_t totalBlocks = 0;
    for (const std::string& inputFile : inputFiles) {
        readers.emplace_back(inputFile.c_str(), O_DIRECT);
        totalBlocks += readers.back().numBlocks;
    }

    LinearObjectWriter writer(outputFile.c_str(), O_DIRECT);
    size_t readersCompleted = 0;
    size_t totalObjects = 0;
    size_t numReaders = readers.size();

    while (readersCompleted < numReaders) {
        size_t minimumReader = -1;
        size_t minimumLength = -1;
        StoreConfig::key_t minimumKey = ~0;
        for (size_t i = 0; i < numReaders; i++) {
            LinearObjectReader<true> &reader = readers[i];
            if (reader.completed) {
                continue;
            }
            StoreConfig::key_t currentKey = reader.currentKey;
            assert(currentKey != minimumKey && "Key collision");
            if (currentKey < minimumKey) {
                minimumKey = currentKey;
                minimumReader = i;
                minimumLength = reader.currentLength;
            }
        }

        LinearObjectReader<true> &minReader = readers[minimumReader];
        writer.write(minimumKey, minimumLength, minReader.currentElementPointer);
        totalObjects++;

        minReader.next();
        if (minReader.hasEnded()) {
            readersCompleted++;
            minReader.completed = true;
        }
        VariableSizeObjectStore::LOG("Merging", writer.blocksGenerated - 1, totalBlocks);
    }

    writer.close();
}
} // Namespace pacthash
