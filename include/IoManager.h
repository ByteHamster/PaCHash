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
constexpr size_t UNBUFFERED_RESERVE_GIGABYTES = 9;

class IoManager {
    public:
        [[nodiscard]] virtual char *enqueueRead(size_t from, size_t length, char *readBuffer) = 0;
        virtual void submit() = 0;
        virtual void awaitCompletion() = 0;
};

extern unsigned char tmpFetchOnly;
unsigned char tmpFetchOnly = 0;
template <int openFlags = 0> // | O_DIRECT | O_SYNC
struct MemoryMapIO : public IoManager {
    private:
        int fd;
        char *file;
        struct stat fileStat = {};
        std::vector<std::tuple<char *, size_t>> ongoingRequests;
    public:
        static std::string NAME() {
            return "MemoryMapIO<" + std::to_string(openFlags) + ">";
        };

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

        [[nodiscard]] char *enqueueRead(size_t from, size_t length, char *readBuffer) final {
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

        void submit() final {
            // Nothing to do
        }

        void awaitCompletion() final {
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
    private:
        char *eatUpRAM = nullptr;
    public:
        static std::string NAME() {
            return "UnbufferedMemoryMapIO<" + std::to_string(openFlags) + ">";
        };

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
struct PosixIO  : public IoManager{
    private:
        int fd;
    public:
        static std::string NAME() {
            return "PosixIO<" + std::to_string(openFlags) + ">";
        };

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

        [[nodiscard]] char *enqueueRead(size_t from, size_t length, char *readBuffer) final {
            assert(from % 4096 == 0);
            assert(length % 4096 == 0);
            assert((size_t)readBuffer % 4096 == 0);
            assert(length > 0);
            lseek(fd, from, SEEK_SET);
            read(fd, readBuffer, length);
            return readBuffer;
        }

        void submit() final {
            // Nothing to do
        }

        void awaitCompletion() final {
            // Nothing to do. Reading is synchronous.
        }
};

template <int openFlags = 0>
struct UnbufferedPosixIO : public PosixIO<openFlags> {
    private:
        char *eatUpRAM = nullptr;
    public:
        static std::string NAME() {
            return "UnbufferedPosixIO<" + std::to_string(openFlags) + ">";
        };

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
struct PosixAIO  : public IoManager {
    private:
        int fd;
        size_t currentRequest = 0;
        static constexpr size_t maxSimultaneousRequests = 2 * PageConfig::MAX_SIMULTANEOUS_QUERIES; // Kind of hacky. Needed for cuckoo.
        struct aiocb aiocbs[maxSimultaneousRequests];
    public:
        static std::string NAME() {
            return "PosixAIO<" + std::to_string(openFlags) + ">";
        };

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

        [[nodiscard]] char *enqueueRead(size_t from, size_t length, char *readBuffer) final {
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

        void submit() final {
            // Nothing to do
        }

        void awaitCompletion() final {
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

#include <linux/aio_abi.h>
#include <sys/syscall.h>
template <int openFlags = 0>
struct LinuxIoSubmit  : public IoManager {
    private:
        int fd;
        size_t currentRequest = 0;
        static constexpr size_t maxSimultaneousRequests = 2 * PageConfig::MAX_SIMULTANEOUS_QUERIES; // Kind of hacky. Needed for cuckoo.
        struct iocb iocbs[maxSimultaneousRequests] = {0};
        struct iocb *list_of_iocb[maxSimultaneousRequests] = {nullptr};
        io_event events[maxSimultaneousRequests] = {0};
        aio_context_t context = 0;
    public:
        static std::string NAME() {
            return "LinuxIoSubmit<" + std::to_string(openFlags) + ">";
        };

        explicit LinuxIoSubmit(const char *filename) {
            fd = open(filename, O_RDONLY | openFlags);
            if (fd < 0) {
                std::cerr<<"Error opening file"<<std::endl;
                exit(1);
            }
            for (int i = 0; i < maxSimultaneousRequests; i++) {
                list_of_iocb[i] = &iocbs[i];
            }
            // io_setup(nr, ctxp)
            int ret = syscall(__NR_io_setup, maxSimultaneousRequests, &context);
            if (ret < 0) {
                fprintf(stderr, "io_setup\n");
                exit(1);
            }
        }

        ~LinuxIoSubmit() {
            close(fd);
            // io_destroy(ctx)
            syscall(__NR_io_destroy, context);
        };

        [[nodiscard]] char *enqueueRead(size_t from, size_t length, char *readBuffer) final {
            assert(from % 4096 == 0);
            assert(length % 4096 == 0);
            assert((uint64_t)&readBuffer[0] % 4096 == 0);
            assert(length > 0);
            assert(currentRequest >= 0 && currentRequest < maxSimultaneousRequests);
            iocbs[currentRequest] = {0};
            iocbs[currentRequest].aio_lio_opcode = IOCB_CMD_PREAD;
            iocbs[currentRequest].aio_buf = (uint64_t)&readBuffer[0];
            iocbs[currentRequest].aio_fildes = fd;
            iocbs[currentRequest].aio_nbytes = length;
            iocbs[currentRequest].aio_offset = from;
            currentRequest++;
            return readBuffer;
        }

        void submit() final {
            // io_submit(ctx, nr, iocbpp)
            int ret = syscall(__NR_io_submit, context, currentRequest, list_of_iocb);
            if (ret != currentRequest) {
                fprintf(stderr, "io_submit %d %s\n", ret, strerror(errno));
                exit(1);
            }
        }

        void awaitCompletion() final {
            // io_getevents(ctx, min_nr, max_nr, events, timeout)
            int ret = syscall(__NR_io_getevents, context, currentRequest, currentRequest, events, nullptr);
            if (ret != currentRequest) {
                fprintf(stderr, "io_getevents\n");
                exit(1);
            }
            while (currentRequest > 0) {
                currentRequest--;
                if (events[currentRequest].res <= 0) {
                    fprintf(stderr, "io_getevents %s\n", std::strerror(events[currentRequest].res));
                    exit(1);
                }
            }
        }
};

#if HAS_LIBURING
#include <liburing.h>
template <int openFlags = 0>
struct UringIO  : public IoManager {
    private:
        int fd;
        size_t currentRequest = 0;
        static constexpr size_t maxSimultaneousRequests = 2 * PageConfig::MAX_SIMULTANEOUS_QUERIES; // Kind of hacky. Needed for cuckoo.
        struct io_uring ring;
        struct iovec *iovecs;
        struct io_uring_sqe *sqe;
        struct io_uring_cqe *cqe;
    public:
        static std::string NAME() {
            return "UringIO<" + std::to_string(openFlags) + ">";
        };

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

        [[nodiscard]] char *enqueueRead(size_t from, size_t length, char *readBuffer) final {
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

        void submit() final {
            int ret = io_uring_submit(&ring);
            if (ret < 0) {
                fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
                exit(1);
            }
        }

        void awaitCompletion() final {
            int ret;
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
