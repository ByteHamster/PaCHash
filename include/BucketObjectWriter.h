#pragma once

class BucketObjectWriter {
    public:
        static void writeBuckets(const char *filename, std::vector<VariableSizeObjectStore::Bucket> buckets, ObjectProvider &objectProvider) {
            size_t numBuckets = buckets.size();

            int fd = open(filename, O_RDWR | O_CREAT, 0600);
            if (fd < 0) {
                std::cerr<<"Error opening output file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            uint64_t fileSize = (numBuckets + 1)*PageConfig::PAGE_SIZE;
            if (ftruncate(fd, fileSize) < 0) {
                std::cerr<<"ftruncate: "<<strerror(errno)<<". If this is a partition, it can be ignored."<<std::endl;
            }
            char *file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
            if (file == MAP_FAILED) {
                std::cerr<<"Map output file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            madvise(file, fileSize, MADV_SEQUENTIAL);

            VariableSizeObjectStore::Item firstMetadataItem = {0, sizeof(VariableSizeObjectStore::MetadataObjectType), 0};
            buckets.at(0).items.insert(buckets.at(0).items.begin(), firstMetadataItem);

            for (int bucketIdx = 0; bucketIdx < numBuckets; bucketIdx++) {
                VariableSizeObjectStore::Bucket &bucket = buckets.at(bucketIdx);
                VariableSizeObjectStore::BlockStorage storage =
                        VariableSizeObjectStore::BlockStorage::init(file + bucketIdx*PageConfig::PAGE_SIZE, 0, bucket.items.size());

                uint16_t numObjectsInBlock = bucket.items.size();

                for (size_t i = 0; i < numObjectsInBlock; i++) {
                    storage.lengths[i] = bucket.items.at(i).length;
                }
                for (size_t i = 0; i < numObjectsInBlock; i++) {
                    storage.keys[i] = bucket.items.at(i).key;
                }

                storage.calculateObjectPositions();
                for (size_t i = 0; i < numObjectsInBlock; i++) {
                    VariableSizeObjectStore::Item &item = bucket.items.at(i);
                    if (item.key == 0) {
                        VariableSizeObjectStore::MetadataObjectType metadataObject = numBuckets;
                        memcpy(storage.objects[i], &metadataObject, sizeof(VariableSizeObjectStore::MetadataObjectType));
                    } else {
                        const char *objectContent = objectProvider.getValue(item.key);
                        assert(item.length <= PageConfig::MAX_OBJECT_SIZE);
                        memcpy(storage.objects[i], objectContent, item.length);
                    }
                }
                VariableSizeObjectStore::LOG("Writing", bucketIdx, numBuckets);
            }
            VariableSizeObjectStore::BlockStorage::init(file + numBuckets * PageConfig::PAGE_SIZE, 0, 0);
            VariableSizeObjectStore::LOG("Flushing and closing file");
            munmap(file, fileSize);
            close(fd);
        }
};
