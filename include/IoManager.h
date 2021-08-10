#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cassert>
#include <iostream>
#include <vector>

#include "PageConfig.h"

// Used for pre-fetching data without having page fault occur later
extern unsigned char tmpFetchOnly;
unsigned char tmpFetchOnly = 0;

constexpr size_t UNBUFFERED_RESERVE_GIGABYTES = 9;

template <int openFlags = 0> // | O_DIRECT | O_SYNC
struct MemoryMapIO {
    static std::string NAME() { return "MemoryMapIO<" + std::to_string(openFlags) + ">"; };
    int fd;
    char *file;
    struct stat fileStat = {};
    std::vector<std::tuple<char *, size_t>> ongoingRequests;

    explicit MemoryMapIO(const char *filename) {
        fd = open(filename, O_RDONLY | openFlags);
        if (fd < 0) {
            std::cerr<<"Error opening file"<<std::endl;
            exit(1);
        }
        fstat(fd, &fileStat);
        file = static_cast<char *>(mmap(nullptr, fileStat.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
        madvise(file, fileStat.st_size, MADV_RANDOM);
    }

    ~MemoryMapIO() {
        munmap(file, fileStat.st_size);
        close(fd);
    };

    [[nodiscard]] char *readBlocks(size_t from, size_t length, char *readBuffer) {
        assert(from % 4096 == 0);
        assert(length % 4096 == 0);
        assert(length > 0);
        char *block = file + from;
        for (int probe = 0; probe < length; probe += PageConfig::PAGE_SIZE / 4) {
            __builtin_prefetch(&block[probe]);
        }
        ongoingRequests.emplace_back(block, length);
        return block;
    }

    void awaitCompletionOfReadRequests() {
        for (const auto& [block, length] : this->ongoingRequests) {
            for (int probe = 0; probe < length; probe += PageConfig::PAGE_SIZE / 4) {
                tmpFetchOnly += block[probe];
            }
        }
        ongoingRequests.clear();
    }
};

template <int openFlags = 0>
struct UnbufferedMemoryMapIO : public MemoryMapIO<openFlags> {
    static std::string NAME() { return "UnbufferedMemoryMapIO<" + std::to_string(openFlags) + ">"; };
    char *eatUpRAM = nullptr;

    explicit UnbufferedMemoryMapIO(const char *filename) : MemoryMapIO<openFlags>(filename) {
        size_t space = UNBUFFERED_RESERVE_GIGABYTES * 1024l * 1024l * 1024l;
        eatUpRAM = static_cast<char *>(malloc(space));
        for (size_t pos = 0; pos < space; pos += 1024) {
            eatUpRAM[pos] = 42;
        }
    }

    ~UnbufferedMemoryMapIO() {
        free(eatUpRAM);
    };
};

template <int openFlags = 0>
struct PosixIO {
    static std::string NAME() { return "PosixIO<" + std::to_string(openFlags) + ">"; };
    int fd;

    explicit PosixIO(const char *filename) {
        fd = open(filename, O_RDONLY | openFlags);
        if (fd < 0) {
            std::cerr<<"Error opening file"<<std::endl;
            exit(1);
        }
    }

    ~PosixIO() {
        close(fd);
    };

    [[nodiscard]] char *readBlocks(size_t from, size_t length, char *readBuffer) const {
        assert(from % 4096 == 0);
        assert(length % 4096 == 0);
        assert((size_t)readBuffer % 4096 == 0);
        assert(length > 0);
        lseek(fd, from, SEEK_SET);
        read(fd, readBuffer, length);
        return readBuffer;
    }

    void awaitCompletionOfReadRequests() {
        // Nothing to do. Reading is synchronous.
    }
};

template <int openFlags = 0>
struct UnbufferedPosixIO : public PosixIO<openFlags> {
    static std::string NAME() { return "UnbufferedPosixIO<" + std::to_string(openFlags) + ">"; };
    char *eatUpRAM = nullptr;

    explicit UnbufferedPosixIO(const char *filename) : PosixIO<openFlags>(filename) {
        size_t space = UNBUFFERED_RESERVE_GIGABYTES * 1024l * 1024l * 1024l;
        eatUpRAM = static_cast<char *>(malloc(space));
        for (size_t pos = 0; pos < space; pos += 1024) {
            eatUpRAM[pos] = 42;
        }
    }

    ~UnbufferedPosixIO() {
        free(eatUpRAM);
    };
};

#ifdef HAS_LIBAIO
#include <aio.h>
template <int openFlags = 0>
struct PosixAIO {
    static std::string NAME() { return "PosixAIO<" + std::to_string(openFlags) + ">"; };
    int fd;
    size_t currentRequest = 0;
    static constexpr size_t maxSimultaneousRequests = 2 * PageConfig::MAX_SIMULTANEOUS_QUERIES; // Kind of hacky. Needed for cuckoo.
    struct aiocb aiocbs[maxSimultaneousRequests];

    explicit PosixAIO(const char *filename) {
        fd = open(filename, O_RDONLY | openFlags);
        if (fd < 0) {
            std::cerr<<"Error opening file"<<std::endl;
            exit(1);
        }
    }

    ~PosixAIO() {
        close(fd);
    };

    [[nodiscard]] char *readBlocks(size_t from, size_t length, char *readBuffer) {
        assert(from % 4096 == 0);
        assert(length % 4096 == 0);
        assert(length > 0);
        assert(currentRequest >= 0 && currentRequest < maxSimultaneousRequests);
        aiocbs[currentRequest] = {0};
        aiocbs[currentRequest].aio_buf = readBuffer;
        aiocbs[currentRequest].aio_fildes = fd;
        aiocbs[currentRequest].aio_nbytes = length;
        aiocbs[currentRequest].aio_offset = from;
        if (aio_read(&aiocbs[currentRequest]) < 0) {
            perror("aio_read");
        }
        currentRequest++;
        return readBuffer;
    }

    void awaitCompletionOfReadRequests() {
        while (currentRequest > 0) {
            currentRequest--;
            while (aio_error(&aiocbs[currentRequest]) == EINPROGRESS); // Wait for result
            if (aio_return(&aiocbs[currentRequest]) < 0) {
                perror("aio_return");
            }
        }
    }
};
#endif // HAS_LIBAIO

#if HAS_LIBURING
#include <liburing.h>
template <int openFlags = 0>
struct UringIO {
    static std::string NAME() { return "UringIO<" + std::to_string(openFlags) + ">"; };
    int fd;
    size_t currentRequest = 0;
    static constexpr size_t maxSimultaneousRequests = 2 * PageConfig::MAX_SIMULTANEOUS_QUERIES; // Kind of hacky. Needed for cuckoo.
    struct io_uring ring;
    struct iovec *iovecs;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;

    explicit UringIO(const char *filename) {
        fd = open(filename, O_RDONLY | openFlags);
        if (fd < 0) {
            std::cerr<<"Error opening file"<<std::endl;
            exit(1);
        }
        int ret = io_uring_queue_init(maxSimultaneousRequests, &ring, 0);
        if (ret < 0) {
            fprintf(stderr, "queue_init: %s\n", strerror(-ret));
            exit(1);
        }
        iovecs = static_cast<iovec *>(calloc(maxSimultaneousRequests, sizeof(struct iovec)));
    }

    ~UringIO() {
        close(fd);
        io_uring_queue_exit(&ring);
        free(iovecs);
    };

    [[nodiscard]] char *readBlocks(size_t from, size_t length, char *readBuffer) {
        iovecs[currentRequest].iov_base = readBuffer;
        iovecs[currentRequest].iov_len = length;
        sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            fprintf(stderr, "io_uring_get_sqe\n");
            exit(1);
        }
        io_uring_prep_readv(sqe, fd, &iovecs[currentRequest], 1, from);
        currentRequest++;
        return readBuffer;
    }

    void awaitCompletionOfReadRequests() {
        int ret;

        ret = io_uring_submit(&ring);
        if (ret < 0) {
            fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
            exit(1);
        }

        while (currentRequest > 0) {
            currentRequest--;
            ret = io_uring_wait_cqe(&ring, &cqe);
            if (ret < 0) {
                fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
                exit(1);
            }
            io_uring_cqe_seen(&ring, cqe);
        }
    }
};
#endif //HAS_LIBURING
