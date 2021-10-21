#include <liburing.h>
#include <cassert>
#include <random>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <tlx/cmdline_parser.hpp>
#include <chrono>
#include <VariableSizeObjectStore.h>
#include <BlockIterator.h>

#define DEPTH 128
#define BATCH_COMPLETE 32
#define BS 4096

int fd;
char *buffer;
struct io_uring *rings = {};
unsigned long blocks = 0;
unsigned int numRings = 1;
unsigned int numQueries = 1000000;

void prep_one(int index, struct io_uring *ring) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    assert(sqe != nullptr);
    io_uring_prep_read(sqe, fd, buffer + index * BS, BS, (lrand48() % blocks)*4096);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void *>(index));
}

unsigned reap_events(struct io_uring *ring) {
    unsigned completed = 0;
    struct io_uring_cqe *cqe = nullptr;
    while (true) {
        io_uring_peek_cqe(ring, &cqe);
        if (cqe == nullptr) {
            return completed;
        }
        assert(cqe->res == BS);
        int index = static_cast<int>(reinterpret_cast<long int>(io_uring_cqe_get_data(cqe)));
        io_uring_cqe_seen(ring, cqe);
        prep_one(index, ring);
        completed++;
    }
    return -1;
}

int randomRead(int argc, char** argv) {
    std::string path = "/dev/nvme1n1";
    size_t maxSize = -1;

    tlx::CmdlineParser cmd;
    cmd.add_string('f', "filename", path, "File to read");
    cmd.add_size_t('s', "max_size", maxSize, "Maximum file size to read, in GB");
    cmd.add_uint('n', "num_rings", numRings, "Number of rings to use");
    cmd.add_uint('q', "num_queries", numQueries, "Number of queries");
    if (!cmd.process(argc, argv)) {
        return 1;
    }

    fd = open(path.c_str(), O_RDONLY | O_DIRECT);
    assert(fd >= 0);
    struct stat st = {};
    if (fstat(fd, &st) < 0)
        assert(false);
    if (S_ISBLK(st.st_mode)) {
        unsigned long long bytes;
        if (ioctl(fd, BLKGETSIZE64, &bytes) != 0) {
            assert(false);
        }
        blocks = bytes / BS;
    } else if (S_ISREG(st.st_mode)) {
        blocks = st.st_size / BS;
    }

    if (maxSize != -1) {
        blocks = std::min(blocks, maxSize*1024L*1024L*1024L/4096L);
    }

    buffer = new (std::align_val_t(BS)) char[DEPTH * BS * numRings];
    rings = new struct io_uring[numRings];
    for (int i = 0; i < numRings; i++) {
        int ret = io_uring_queue_init(DEPTH, &rings[i], IORING_SETUP_IOPOLL);
        assert(ret == 0);
    }
    for (int i = 0; i < DEPTH * numRings; i++) {
        prep_one(i, &rings[i%numRings]);
    }
    for (int i = 0; i < numRings; i++) {
        int ret = io_uring_submit(&rings[i]);
        assert(ret >= 0);
    }
    struct timeval begin = {};
    struct timeval end = {};
    gettimeofday(&begin, nullptr);

    int i = 0;
    int done = 0;
    while (done < numQueries) {
        unsigned reaped = reap_events(&rings[i % numRings]);
        int ret = io_uring_submit_and_wait(&rings[i % numRings], BATCH_COMPLETE);
        assert(ret == reaped);
        done += ret;
        i++;
    }

    gettimeofday(&end, nullptr);
    long seconds = end.tv_sec - begin.tv_sec;
    long microseconds = end.tv_usec - begin.tv_usec;
    double elapsed = seconds + microseconds * 1e-6;
    unsigned long iops = 1.0 / elapsed * done;
    printf("RESULT rings=%u blocks=%lu iops=%lu\n", numRings, blocks, iops);

    delete[] buffer;
    delete[] rings;
    return 0;
}

int linearRead(int argc, char** argv) {
    size_t fileSize = 10L * 1024L * 1024L * 1024L; // 100 GB
    size_t numBlocks = fileSize / PageConfig::PAGE_SIZE;
    const char *filename = "/dev/nvme1n1";
    {
        auto queryStart = std::chrono::high_resolution_clock::now();
        int fd = open(filename, O_RDONLY);
        char *file = static_cast<char *>(mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0));
        madvise(file, fileSize, MADV_SEQUENTIAL);
        size_t objectsFound = 0;
        for (size_t bucket = 0; bucket < numBlocks; bucket++) {
            VariableSizeObjectStore::BlockStorage block(file + PageConfig::PAGE_SIZE * bucket);
            objectsFound += block.numObjects;
        }
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=mmap objects=" << objectsFound << " time=" << timeMilliseconds
                    << " iops=" << (fileSize / PageConfig::PAGE_SIZE)*1000/timeMilliseconds << std::endl;
    }
    {
        size_t depth = 128;
        auto queryStart = std::chrono::high_resolution_clock::now();
        UringIO ioManager(filename, O_RDONLY | O_DIRECT, depth);
        char *buffer1 = new (std::align_val_t(PageConfig::PAGE_SIZE)) char[depth * PageConfig::PAGE_SIZE];
        char *buffer2 = new (std::align_val_t(PageConfig::PAGE_SIZE)) char[depth * PageConfig::PAGE_SIZE];

        // Read first requests to buffer2
        for (int i = 0; i < depth; i++) {
            ioManager.enqueueRead(buffer2 + i*PageConfig::PAGE_SIZE, i * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, 0);
        }
        ioManager.submit();

        size_t objectsFound = 0;
        for (size_t bucket = 0; bucket < numBlocks; bucket++) {
            if (bucket % depth == 0) {
                std::swap(buffer1, buffer2);
                for (int i = 0; i < depth; i++) {
                    ioManager.awaitAny();
                }
                for (int i = 0; i < depth && bucket+i+depth < numBlocks; i++) {
                    ioManager.enqueueRead(buffer2 + i*PageConfig::PAGE_SIZE, (bucket + i + depth) * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, 0);
                }
                ioManager.submit();
            }

            VariableSizeObjectStore::BlockStorage block(buffer1 + PageConfig::PAGE_SIZE * (bucket % depth));
            objectsFound += block.numObjects;
        }

        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=uringBatched objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << (fileSize / PageConfig::PAGE_SIZE)*1000/timeMilliseconds << std::endl;
        delete[] buffer1;
        delete[] buffer2;
    }
    {
        size_t depth = 128;
        auto queryStart = std::chrono::high_resolution_clock::now();
        UringIO ioManager(filename, O_RDONLY | O_DIRECT, depth);
        char *buffer = new (std::align_val_t(PageConfig::PAGE_SIZE)) char[depth * PageConfig::PAGE_SIZE];

        size_t loadNext = 0;
        // Read first requests to buffer2
        for (int i = 0; i < depth; i++) {
            ioManager.enqueueRead(buffer + i*PageConfig::PAGE_SIZE, loadNext * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, i + 1);
            loadNext++;
        }
        ioManager.submit();

        size_t objectsFound = 0;
        for (size_t bucket = 0; bucket < numBlocks; bucket++) {
            size_t name = ioManager.peekAny();
            if (name == 0) {
                // Re-submit
                ioManager.submit();
                name = ioManager.awaitAny();
            }

            VariableSizeObjectStore::BlockStorage block(buffer + PageConfig::PAGE_SIZE * (name-1));
            objectsFound += block.numObjects;
            if (loadNext < numBlocks) {
                ioManager.enqueueRead(buffer + (name-1)*PageConfig::PAGE_SIZE, loadNext * PageConfig::PAGE_SIZE, PageConfig::PAGE_SIZE, name);
                loadNext++;
            }
        }

        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=uringAny objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << (fileSize / PageConfig::PAGE_SIZE)*1000/timeMilliseconds << std::endl;
        delete[] buffer;
    }
    {
        MemoryMapBlockIterator iterator(filename, fileSize);
        auto queryStart = std::chrono::high_resolution_clock::now();
        size_t objectsFound = 0;
        for (size_t bucket = 0; bucket < numBlocks; bucket++) {
            VariableSizeObjectStore::BlockStorage block(iterator.bucketContent());
            objectsFound += block.numObjects;
            if (bucket < numBlocks - 1) {
                iterator.next();
            }
        }
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=iteratorMmap objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << (fileSize / PageConfig::PAGE_SIZE)*1000/timeMilliseconds << std::endl;
    }
    {
        UringAnyBlockIterator iterator(filename, 128, numBlocks, true, O_DIRECT);
        auto queryStart = std::chrono::high_resolution_clock::now();
        size_t objectsFound = 0;
        for (size_t bucket = 0; bucket < numBlocks; bucket++) {
            VariableSizeObjectStore::BlockStorage block(iterator.bucketContent());
            objectsFound += block.numObjects;
            if (bucket < numBlocks - 1) {
                iterator.next();
            }
        }
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=iteratorUring objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << (fileSize / PageConfig::PAGE_SIZE)*1000/timeMilliseconds << std::endl;
    }
    return 0;
}

int main(int argc, char** argv) {
    return linearRead(argc, argv);
}