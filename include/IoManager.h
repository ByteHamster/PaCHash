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
#include <tuple>

#include "PageConfig.h"

// Used for pre-fetching data without having page fault occur later
constexpr size_t UNBUFFERED_RESERVE_GIGABYTES = 9;

class IoManager {
    protected:
        int maxLength;
        int maxSimultaneousRequests;
        size_t currentRequest = 0;
        char *readBuffer;
        int openFlags;
    public:
        IoManager (int openFlags, int maxSimultaneousRequests, int maxLength)
                : maxLength(maxLength), maxSimultaneousRequests(maxSimultaneousRequests), openFlags(openFlags) {
            readBuffer = static_cast<char *>(aligned_alloc(PageConfig::PAGE_SIZE, maxSimultaneousRequests * maxLength * sizeof(char)));
        }

        ~IoManager() {
            free(readBuffer);
        }

        [[nodiscard]] virtual char *enqueueRead(size_t from, size_t length) = 0;
        virtual void submit() = 0;
        virtual void awaitCompletion() = 0;
};

extern unsigned char tmpFetchOnly;
unsigned char tmpFetchOnly = 0;
struct MemoryMapIO : public IoManager {
    private:
        int fd;
        char *file;
        struct stat fileStat = {};
        std::vector<std::tuple<char *, size_t>> ongoingRequests;
    public:
        std::string name() {
            return "MemoryMapIO<" + std::to_string(openFlags) + ">";
        };

        explicit MemoryMapIO(int openFlags, int maxSimultaneousRequests, int maxLength, const char *filename)
                : IoManager(openFlags, maxSimultaneousRequests, maxLength) {
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

        [[nodiscard]] char *enqueueRead(size_t from, size_t length) final {
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

struct UnbufferedMemoryMapIO : public MemoryMapIO {
    private:
        char *eatUpRAM = nullptr;
    public:
        std::string name() {
            return "UnbufferedMemoryMapIO<" + std::to_string(openFlags) + ">";
        };

        explicit UnbufferedMemoryMapIO(int openFlags, int maxSimultaneousRequests, int maxLength, const char *filename)
                : MemoryMapIO(openFlags, maxSimultaneousRequests, maxLength, filename) {
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

struct PosixIO  : public IoManager {
    private:
        int fd;
    public:
        std::string name() {
            return "PosixIO<" + std::to_string(openFlags) + ">";
        };

        explicit PosixIO(int openFlags, int maxSimultaneousRequests, int maxLength, const char *filename)
                : IoManager(openFlags, maxSimultaneousRequests, maxLength) {
            fd = open(filename, O_RDONLY | openFlags);
            if (fd < 0) {
                std::cerr<<"Error opening file"<<std::endl;
                exit(1);
            }
        }

        ~PosixIO() {
            close(fd);
        };

        [[nodiscard]] char *enqueueRead(size_t from, size_t length) final {
            assert(from % 4096 == 0);
            assert(length % 4096 == 0);
            assert(length > 0);
            assert(length <= maxLength);
            char *buf = readBuffer + currentRequest*maxLength;
            size_t read = pread(fd, buf, length, from);
            if (read != length) {
                fprintf(stderr, "pread %s\n", strerror(errno));
                exit(1);
            }
            currentRequest++;
            return buf;
        }

        void submit() final {
            // Nothing to do
        }

        void awaitCompletion() final {
            // Nothing to do. Reading is synchronous.
            currentRequest = 0;
        }
};

struct UnbufferedPosixIO : public PosixIO {
    private:
        char *eatUpRAM = nullptr;
    public:
        std::string name() {
            return "UnbufferedPosixIO<" + std::to_string(openFlags) + ">";
        };

        explicit UnbufferedPosixIO(int openFlags, int maxSimultaneousRequests, int maxLength, const char *filename)
                : PosixIO(openFlags, maxSimultaneousRequests, maxLength, filename) {
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
struct PosixAIO  : public IoManager {
    private:
        int fd;
        struct aiocb *aiocbs;
    public:
        std::string name() {
            return "PosixAIO<" + std::to_string(openFlags) + ">";
        };

        explicit PosixAIO(int openFlags, int maxSimultaneousRequests, int maxLength, const char *filename)
                : IoManager(openFlags, maxSimultaneousRequests, maxLength) {
            fd = open(filename, O_RDONLY | openFlags);
            if (fd < 0) {
                std::cerr<<"Error opening file"<<std::endl;
                exit(1);
            }
            aiocbs = static_cast<aiocb *>(malloc(maxSimultaneousRequests * sizeof(struct aiocb)));
        }

        ~PosixAIO() {
            close(fd);
            free(aiocbs);
        };

        [[nodiscard]] char *enqueueRead(size_t from, size_t length) final {
            assert(currentRequest < maxSimultaneousRequests);
            assert(from % 4096 == 0);
            assert(length % 4096 == 0);
            assert(length > 0);
            assert(currentRequest >= 0 && currentRequest < maxSimultaneousRequests);
            char *buf = readBuffer + currentRequest*maxLength;
            aiocbs[currentRequest] = {0};
            aiocbs[currentRequest].aio_buf = buf;
            aiocbs[currentRequest].aio_fildes = fd;
            aiocbs[currentRequest].aio_nbytes = length;
            aiocbs[currentRequest].aio_offset = from;
            if (aio_read(&aiocbs[currentRequest]) < 0) {
                perror("aio_read");
                exit(1);
            }
            currentRequest++;
            return buf;
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
                    exit(1);
                }
            }
        }
};
#endif // HAS_LIBAIO

#include <linux/aio_abi.h>
#include <sys/syscall.h>
struct LinuxIoSubmit  : public IoManager {
    private:
        int fd;
        struct iocb *iocbs;
        struct iocb **list_of_iocb;
        io_event *events;
        aio_context_t context = 0;
    public:
        std::string name() {
            return "LinuxIoSubmit<" + std::to_string(openFlags) + ">";
        };

        explicit LinuxIoSubmit(int openFlags, int maxSimultaneousRequests, int maxLength, const char *filename)
                : IoManager(openFlags, maxSimultaneousRequests, maxLength) {
            fd = open(filename, O_RDONLY | openFlags);
            if (fd < 0) {
                std::cerr<<"Error opening file"<<std::endl;
                exit(1);
            }
            iocbs = static_cast<iocb *>(malloc(maxSimultaneousRequests * sizeof(struct iocb)));
            list_of_iocb = static_cast<iocb **>(malloc(maxSimultaneousRequests * sizeof(struct iocb*)));
            events = static_cast<io_event *>(malloc(maxSimultaneousRequests * sizeof(io_event)));

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
            free(iocbs);
            free(list_of_iocb);
            free(events);
        };

        [[nodiscard]] char *enqueueRead(size_t from, size_t length) final {
            assert(currentRequest < maxSimultaneousRequests);
            assert(from % 4096 == 0);
            assert(length % 4096 == 0);
            assert(length > 0);
            assert(currentRequest >= 0 && currentRequest < maxSimultaneousRequests);
            char *buf = readBuffer + currentRequest*maxLength;
            iocbs[currentRequest] = {0};
            iocbs[currentRequest].aio_lio_opcode = IOCB_CMD_PREAD;
            iocbs[currentRequest].aio_buf = (uint64_t)&buf[0];
            iocbs[currentRequest].aio_fildes = fd;
            iocbs[currentRequest].aio_nbytes = length;
            iocbs[currentRequest].aio_offset = from;
            currentRequest++;
            return buf;
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
struct UringIO  : public IoManager {
    private:
        int fd;
        struct io_uring ring;
        struct iovec *iovecs;
        struct io_uring_sqe *sqe;
        struct io_uring_cqe *cqe;
    public:
        std::string name() {
            return "UringIO<" + std::to_string(openFlags) + ">";
        };

        explicit UringIO(int openFlags, int maxSimultaneousRequests, int maxLength, const char *filename)
                : IoManager(openFlags, maxSimultaneousRequests, maxLength) {
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

        [[nodiscard]] char *enqueueRead(size_t from, size_t length) final {
            assert(currentRequest < maxSimultaneousRequests);
            char *buf = readBuffer + currentRequest*maxLength;
            iovecs[currentRequest].iov_base = buf;
            iovecs[currentRequest].iov_len = length;
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                fprintf(stderr, "io_uring_get_sqe\n");
                exit(1);
            }
            io_uring_prep_readv(sqe, fd, &iovecs[currentRequest], 1, from);
            currentRequest++;
            return buf;
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
