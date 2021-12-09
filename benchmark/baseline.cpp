#include <liburing.h>
#include <cassert>
#include <random>
#include <unistd.h>
#include <fcntl.h>
#include <tlx/cmdline_parser.hpp>
#include <tlx/math/round_to_power_of_two.hpp>
#include <chrono>
#include <VariableSizeObjectStore.h>
#include <BlockIterator.h>
#include <Util.h>

#define DEPTH 128
#define BATCH_COMPLETE 32

std::string filename = "/dev/nvme1n1";
int fd;
char *buffer;
struct io_uring *rings = {};
size_t blocks = 0;
size_t numRings = 1;
size_t numQueries = 1000000;
size_t blockSize = 4096;
bool nopoll = false;

void prep_one(size_t index, struct io_uring *ring) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    assert(sqe != nullptr);
    io_uring_prep_read(sqe, fd, buffer + index * blockSize, blockSize, (lrand48() % blocks)*blockSize);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void *>(index));
}

size_t reap_events(struct io_uring *ring) {
    size_t completed = 0;
    struct io_uring_cqe *cqe = nullptr;
    while (true) {
        io_uring_peek_cqe(ring, &cqe);
        if (cqe == nullptr) {
            return completed;
        }
        if (cqe->res != blockSize) {
            throw std::ios_base::failure("io_uring_peek_cqe: " + std::string(strerror(-cqe->res)));
        }
        size_t index = reinterpret_cast<size_t>(io_uring_cqe_get_data(cqe));
        io_uring_cqe_seen(ring, cqe);
        prep_one(index, ring);
        completed++;
    }
}

void randomRead() {
    buffer = new (std::align_val_t(tlx::round_up_to_power_of_two(blockSize))) char[DEPTH * blockSize * numRings];
    rings = new struct io_uring[numRings];
    for (size_t i = 0; i < numRings; i++) {
        int ret = io_uring_queue_init(DEPTH, &rings[i], nopoll ? 0 : IORING_SETUP_IOPOLL);
        if (ret != 0) {
            throw std::ios_base::failure("io_uring_queue_init: " + std::string(strerror(-ret)));
        }
    }
    for (size_t i = 0; i < DEPTH * numRings; i++) {
        prep_one(i, &rings[i%numRings]);
    }
    for (size_t i = 0; i < numRings; i++) {
        int ret = io_uring_submit(&rings[i]);
        if (ret < 0) {
            throw std::ios_base::failure("io_uring_submit: " + std::string(strerror(-ret)));
        }
    }
    auto queryStart = std::chrono::high_resolution_clock::now();

    size_t ringRoundRobin = 0;
    size_t requestsDone = 0;
    while (requestsDone < numQueries) {
        size_t reaped = reap_events(&rings[ringRoundRobin % numRings]);
        int ret = io_uring_submit_and_wait(&rings[ringRoundRobin % numRings], BATCH_COMPLETE);
        if (size_t(ret) != reaped) {
            throw std::ios_base::failure("io_uring_submit_and_wait: " + std::string(strerror(-ret)));
        }
        requestsDone += ret;
        ringRoundRobin++;
    }

    auto queryEnd = std::chrono::high_resolution_clock::now();
    long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
    size_t iops = 1000 * requestsDone / timeMilliseconds;
    printf("RESULT rings=%lu blocks=%lu block_size=%lu iops=%lu\n", numRings, blocks, blockSize, iops);

    delete[] buffer;
    delete[] rings;
}

void linearRead() {
    {
        auto queryStart = std::chrono::high_resolution_clock::now();
        char *file = static_cast<char *>(mmap(nullptr, blocks*pacthash::StoreConfig::BLOCK_LENGTH, PROT_READ, MAP_PRIVATE, fd, 0));
        madvise(file, blocks*pacthash::StoreConfig::BLOCK_LENGTH, MADV_SEQUENTIAL);
        size_t objectsFound = 0;
        for (size_t block = 0; block < blocks; block++) {
            pacthash::VariableSizeObjectStore::BlockStorage blockStorage(file + pacthash::StoreConfig::BLOCK_LENGTH * block);
            objectsFound += blockStorage.numObjects;
        }
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=mmap objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << blocks * 1000 / timeMilliseconds << std::endl;
    }
    {
        size_t depth = 128;
        auto queryStart = std::chrono::high_resolution_clock::now();
        pacthash::UringIO ioManager(filename.c_str(), O_RDONLY | O_DIRECT, depth);
        char *buffer1 = new (std::align_val_t(pacthash::StoreConfig::BLOCK_LENGTH)) char[depth * pacthash::StoreConfig::BLOCK_LENGTH];
        char *buffer2 = new (std::align_val_t(pacthash::StoreConfig::BLOCK_LENGTH)) char[depth * pacthash::StoreConfig::BLOCK_LENGTH];

        // Read first requests to buffer2
        for (size_t i = 0; i < depth; i++) {
            ioManager.enqueueRead(buffer2 + i * pacthash::StoreConfig::BLOCK_LENGTH, i * pacthash::StoreConfig::BLOCK_LENGTH, pacthash::StoreConfig::BLOCK_LENGTH, 0);
        }
        ioManager.submit();

        size_t objectsFound = 0;
        for (size_t block = 0; block < blocks; block++) {
            if (block % depth == 0) {
                std::swap(buffer1, buffer2);
                for (size_t i = 0; i < depth && block + i < blocks; i++) {
                    ioManager.awaitAny();
                }
                for (size_t i = 0; i < depth && block + i + depth < blocks; i++) {
                    ioManager.enqueueRead(buffer2 + i * pacthash::StoreConfig::BLOCK_LENGTH,
                      (block + i + depth) * pacthash::StoreConfig::BLOCK_LENGTH, pacthash::StoreConfig::BLOCK_LENGTH, 0);
                }
                ioManager.submit();
            }

            pacthash::VariableSizeObjectStore::BlockStorage blockStorage(buffer1 + pacthash::StoreConfig::BLOCK_LENGTH * (block % depth));
            objectsFound += blockStorage.numObjects;
        }

        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=uringBatched objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << blocks * 1000 / timeMilliseconds << std::endl;
        delete[] buffer1;
        delete[] buffer2;
    }
    {
        size_t depth = 128;
        auto queryStart = std::chrono::high_resolution_clock::now();
        pacthash::UringIO ioManager(filename.c_str(), O_RDONLY | O_DIRECT, depth);
        char *buffer = new (std::align_val_t(pacthash::StoreConfig::BLOCK_LENGTH)) char[depth * pacthash::StoreConfig::BLOCK_LENGTH];

        size_t loadNext = 0;
        // Read first requests to buffer2
        for (size_t i = 0; i < depth; i++) {
            ioManager.enqueueRead(buffer + i * pacthash::StoreConfig::BLOCK_LENGTH,
              loadNext * pacthash::StoreConfig::BLOCK_LENGTH, pacthash::StoreConfig::BLOCK_LENGTH, i + 1);
            loadNext++;
        }
        ioManager.submit();

        size_t objectsFound = 0;
        for (size_t block = 0; block < blocks; block++) {
            size_t name = ioManager.peekAny();
            if (name == 0) {
                // Re-submit
                ioManager.submit();
                name = ioManager.awaitAny();
            }

            pacthash::VariableSizeObjectStore::BlockStorage blockStorage(buffer + pacthash::StoreConfig::BLOCK_LENGTH * (name - 1));
            objectsFound += blockStorage.numObjects;
            if (loadNext < blocks) {
                ioManager.enqueueRead(buffer + (name-1) * pacthash::StoreConfig::BLOCK_LENGTH,
                      loadNext * pacthash::StoreConfig::BLOCK_LENGTH, pacthash::StoreConfig::BLOCK_LENGTH, name);
                loadNext++;
            }
        }

        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=uringAny objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << blocks * 1000 / timeMilliseconds << std::endl;
        delete[] buffer;
    }
    {
        pacthash::MemoryMapBlockIterator iterator(filename.c_str(), blocks * pacthash::StoreConfig::BLOCK_LENGTH);
        auto queryStart = std::chrono::high_resolution_clock::now();
        size_t objectsFound = 0;
        for (size_t block = 0; block < blocks; block++) {
            pacthash::VariableSizeObjectStore::BlockStorage blockStorage(iterator.blockContent());
            objectsFound += blockStorage.numObjects;
            if (block < blocks - 1) {
                iterator.next();
            }
        }
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=iteratorMmap objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << blocks * 1000 / timeMilliseconds << std::endl;
    }
    {
        pacthash::UringAnyBlockIterator iterator(filename.c_str(), 128, blocks, true, O_DIRECT);
        auto queryStart = std::chrono::high_resolution_clock::now();
        size_t objectsFound = 0;
        for (size_t block = 0; block < blocks; block++) {
            pacthash::VariableSizeObjectStore::BlockStorage blockStorage(iterator.blockContent());
            objectsFound += blockStorage.numObjects;
            if (block < blocks - 1) {
                iterator.next();
            }
        }
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=iteratorUring objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << blocks * 1000 / timeMilliseconds << std::endl;
    }
    {
        pacthash::UringDoubleBufferBlockIterator iterator(filename.c_str(), blocks, 255, O_DIRECT);
        auto queryStart = std::chrono::high_resolution_clock::now();
        size_t objectsFound = 0;
        for (size_t block = 0; block < blocks; block++) {
            pacthash::VariableSizeObjectStore::BlockStorage blockStorage(iterator.blockContent());
            objectsFound += blockStorage.numObjects;
            if (block < blocks - 1) {
                iterator.next();
            }
        }
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=iteratorUringDoubleBuffer objects=" << objectsFound << " time=" << timeMilliseconds
                  << " iops=" << blocks * 1000 / timeMilliseconds << std::endl;
    }
}

void linearWrite() {
    {
        auto queryStart = std::chrono::high_resolution_clock::now();
        char *file = static_cast<char *>(mmap(nullptr, blocks * pacthash::StoreConfig::BLOCK_LENGTH, PROT_READ | PROT_WRITE, MAP_SHARED, fd,0));
        madvise(file, blocks * pacthash::StoreConfig::BLOCK_LENGTH, MADV_SEQUENTIAL);
        for (size_t i = 0; i < blocks; i++) {
            memset(&file[i*pacthash::StoreConfig::BLOCK_LENGTH], 42, pacthash::StoreConfig::BLOCK_LENGTH);
        }
        sync();
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=mmap time=" << timeMilliseconds << " pagesPerSecond=" << blocks * 1000 / timeMilliseconds << std::endl;
    }
    {
        size_t blocksPerBatch = 250;
        char *buffer1 = new (std::align_val_t(pacthash::StoreConfig::BLOCK_LENGTH)) char[blocksPerBatch * pacthash::StoreConfig::BLOCK_LENGTH];
        char *buffer2 = new (std::align_val_t(pacthash::StoreConfig::BLOCK_LENGTH)) char[blocksPerBatch * pacthash::StoreConfig::BLOCK_LENGTH];
        pacthash::UringIO ioManager(filename.c_str(), O_RDWR | O_DIRECT | O_CREAT, 2);
        auto queryStart = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < blocks; i++) {
            if (i % blocksPerBatch == 0 && i != 0) {
                if (i != blocksPerBatch) {
                    ioManager.awaitAny();
                }
                std::swap(buffer1, buffer2);
                int result = ftruncate(fd, i * pacthash::StoreConfig::BLOCK_LENGTH);
                (void) result;
                ioManager.enqueueWrite(buffer2, (i - blocksPerBatch) * pacthash::StoreConfig::BLOCK_LENGTH,
                                       blocksPerBatch*pacthash::StoreConfig::BLOCK_LENGTH, 0);
                ioManager.submit();
            }
            memset(&buffer1[(i % blocksPerBatch)*pacthash::StoreConfig::BLOCK_LENGTH], 42, pacthash::StoreConfig::BLOCK_LENGTH);
        }
        ioManager.enqueueWrite(buffer1, (blocks - (blocks % blocksPerBatch)) * pacthash::StoreConfig::BLOCK_LENGTH,
                               (blocks % blocksPerBatch)*pacthash::StoreConfig::BLOCK_LENGTH, 0);
        ioManager.submit();
        ioManager.awaitAny();
        ioManager.awaitAny();
        sync();
        auto queryEnd = std::chrono::high_resolution_clock::now();
        long timeMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(queryEnd - queryStart).count();
        std::cout << "RESULT method=doubleBuffer time=" << timeMilliseconds << " pagesPerSecond=" << blocks * 1000 / timeMilliseconds << std::endl;
    }
}

int main(int argc, char** argv) {
    size_t maxSize = ~0ul;
    bool linear = false;
    bool write = false;

    tlx::CmdlineParser cmd;
    cmd.add_string('f', "filename", filename, "File to read");
    cmd.add_bytes('s', "max_size", maxSize, "Maximum file size to read, supports SI units");
    cmd.add_size_t('n', "num_rings", numRings, "Number of rings to use");
    cmd.add_bytes('q', "num_queries", numQueries, "Number of queries");
    cmd.add_flag('l', "linear", linear, "Read all blocks linearly with different methods");
    cmd.add_flag('w', "write", write, "Write a file with different methods");
    cmd.add_size_t('b', "block_size", blockSize, "Block size per query");
    cmd.add_flag('p', "nopoll", nopoll, "Disable polling IO");

    if (!cmd.process(argc, argv)) {
        return 1;
    }

    fd = open(filename.c_str(), (write ? O_RDWR : O_RDONLY) | O_DIRECT);
    if (fd < 0) {
        throw std::ios_base::failure("Unable to open " + std::string(filename)
                                     + ": " + std::string(strerror(errno)));
    }
    blocks = pacthash::filesize(fd) / blockSize - 1;

    if (maxSize != ~0ul) {
        blocks = std::min(blocks, maxSize/pacthash::StoreConfig::BLOCK_LENGTH);
    }

    if (linear) {
        linearRead();
    } else if (write) {
        linearWrite();
    } else {
        randomRead();
    }
    return 0;
}