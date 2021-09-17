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
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <queue>
#include <unordered_set>

#include "PageConfig.h"

class GetAnyVector {
    private:
        std::vector<bool> occupied;
        int busyRotator = 0;
        int size;
    public:
        explicit GetAnyVector(int size) : size(size) {
            occupied.resize(size);
        }

        int getAnyFreeAndMarkBusy() {
            while (occupied.at(++busyRotator % size)) { }
            occupied.at(busyRotator % size) = true;
            return busyRotator % size;
        }

        int getAnyBusy() {
            while (!occupied.at(++busyRotator % size)) { }
            return busyRotator % size;
        }

        void markFree(int index) {
            occupied.at(index) = false;
        }
};

class IoManager {
    protected:
        size_t maxSimultaneousRequests;
        int fd;
    public:
        IoManager (const char *filename, int openFlags, size_t maxSimultaneousRequests)
                : maxSimultaneousRequests(maxSimultaneousRequests) {
            fd = open(filename, O_RDONLY | openFlags);
            if (fd < 0) {
                std::cerr<<"Error opening file: "<<strerror(errno)<<std::endl;
                exit(1);
            }
        }

        virtual ~IoManager() {
            close(fd);
        }

        virtual std::string name() = 0;
        virtual void enqueueRead(char *dest, size_t offset, size_t length, uint64_t name) = 0;
        virtual uint64_t awaitAny() = 0;
};

struct PosixIO : public IoManager {
    private:
        std::queue<uint64_t> ongoingRequests;
    public:
        std::string name() final {
            return "PosixIO";
        };

        explicit PosixIO(const char *filename, int openFlags, size_t maxSimultaneousRequests)
                : IoManager(filename, openFlags, maxSimultaneousRequests) {
        }

        void enqueueRead(char *dest, size_t offset, size_t length, uint64_t name) final {
            assert(reinterpret_cast<size_t>(dest) % 4096 == 0);
            assert(offset % 4096 == 0);
            assert(length % 4096 == 0);
            assert(length > 0);
            ssize_t read = pread(fd, dest, length, offset);
            if (read <= 0) {
                std::cerr<<"pread: "<<strerror(errno)<<std::endl;
                exit(1);
            }
            ongoingRequests.push(name);
        }

        uint64_t awaitAny() final {
            uint64_t front = ongoingRequests.front();
            ongoingRequests.pop();
            return front;
        }
};

#ifdef HAS_LIBAIO
#include <aio.h>
struct PosixAIO : public IoManager {
    private:
        std::vector<struct aiocb> aiocbs;
        std::vector<uint64_t> names;
        GetAnyVector usedAiocbs;
    public:
        std::string name() final {
            return "PosixAIO";
        };

        explicit PosixAIO(const char *filename, int openFlags, size_t maxSimultaneousRequests)
                : IoManager(filename, openFlags, maxSimultaneousRequests), usedAiocbs(maxSimultaneousRequests) {
            aiocbs.resize(maxSimultaneousRequests);
            names.resize(maxSimultaneousRequests);
        }

        void enqueueRead(char *dest, size_t offset, size_t length, uint64_t name) final {
            assert(reinterpret_cast<size_t>(dest) % 4096 == 0);
            assert(offset % 4096 == 0);
            assert(length % 4096 == 0);
            assert(length > 0);

            int currentAiocb = usedAiocbs.getAnyFreeAndMarkBusy();
            aiocb *aiocb = &aiocbs.at(currentAiocb);
            aiocb->aio_buf = dest;
            aiocb->aio_fildes = fd;
            aiocb->aio_nbytes = length;
            aiocb->aio_offset = offset;
            names.at(currentAiocb) = name;

            if (aio_read(aiocb) < 0) {
                perror("aio_read");
                exit(1);
            }
        }

        uint64_t awaitAny() final {
            while (true) {
                int anyAiocb = usedAiocbs.getAnyBusy();
                if (aio_error(&aiocbs[anyAiocb]) == EINPROGRESS) {
                    continue; // Continue waiting
                }
                // Found one!
                usedAiocbs.markFree(anyAiocb);
                if (aio_return(&aiocbs[anyAiocb]) < 0) {
                    perror("aio_return");
                    exit(1);
                }
                return names.at(anyAiocb);
            }
            return 0;
        }
};
#endif // HAS_LIBAIO

#include <linux/aio_abi.h>
#include <sys/syscall.h>
struct LinuxIoSubmit : public IoManager {
    private:
        struct iocb *iocbs;
        struct iocb **list_of_iocb;
        io_event *events;
        aio_context_t context = 0;
        GetAnyVector usedIocbs;
        std::vector<uint64_t> names;
    public:
        std::string name() final {
            return "LinuxIoSubmit";
        };

        explicit LinuxIoSubmit(const char *filename, int openFlags, size_t maxSimultaneousRequests)
                : IoManager(filename, openFlags, maxSimultaneousRequests), usedIocbs(maxSimultaneousRequests) {
            iocbs = static_cast<iocb *>(malloc(maxSimultaneousRequests * sizeof(struct iocb)));
            list_of_iocb = static_cast<iocb **>(malloc(maxSimultaneousRequests * sizeof(struct iocb*)));
            events = static_cast<io_event *>(malloc(maxSimultaneousRequests * sizeof(io_event)));
            names.resize(maxSimultaneousRequests);

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

        ~LinuxIoSubmit() override {
            // io_destroy(ctx)
            syscall(__NR_io_destroy, context);
            free(iocbs);
            free(list_of_iocb);
            free(events);
        };

        void enqueueRead(char *dest, size_t offset, size_t length, uint64_t name) final {
            assert(reinterpret_cast<size_t>(dest) % 4096 == 0);
            assert(offset % 4096 == 0);
            assert(length % 4096 == 0);
            assert(length > 0);
            int anyIocb = usedIocbs.getAnyFreeAndMarkBusy();
            iocbs[anyIocb] = {0};
            iocbs[anyIocb].aio_lio_opcode = IOCB_CMD_PREAD;
            iocbs[anyIocb].aio_buf = (uint64_t)dest;
            iocbs[anyIocb].aio_fildes = fd;
            iocbs[anyIocb].aio_nbytes = length;
            iocbs[anyIocb].aio_offset = offset;
            iocbs[anyIocb].aio_data = anyIocb;
            names.at(anyIocb) = name;

            // io_submit(ctx, nr, iocbpp)
            int ret = syscall(__NR_io_submit, context, 1, list_of_iocb + anyIocb);
            if (ret != 1) {
                fprintf(stderr, "io_submit %d %s\n", ret, strerror(errno));
                exit(1);
            }
        }

        uint64_t awaitAny() final {
            // io_getevents(ctx, min_nr, max_nr, events, timeout)
            int ret = syscall(__NR_io_getevents, context, 1, 1, events, nullptr);
            if (ret != 1) {
                fprintf(stderr, "io_getevents\n");
                exit(1);
            }
            if (events[0].res <= 0) {
                fprintf(stderr, "io_getevents %s\n", std::strerror(events[0].res));
                exit(1);
            }
            int index = events[0].data;
            usedIocbs.markFree(index);
            return names.at(index);
        }
};

#if HAS_LIBURING
#include <liburing.h>
struct UringIO  : public IoManager {
    private:
        struct io_uring ring = {};
    public:
        std::string name() final {
            return "UringIO";
        };

        explicit UringIO(const char *filename, int openFlags, size_t maxSimultaneousRequests)
                : IoManager(filename, openFlags, maxSimultaneousRequests) {
            int ret = io_uring_queue_init(maxSimultaneousRequests, &ring, 0);
            if (ret < 0) {
                fprintf(stderr, "queue_init: %s\n", strerror(-ret));
                exit(1);
            }
        }

        ~UringIO() override {
            close(fd);
            io_uring_queue_exit(&ring);
        };

        void enqueueRead(char *dest, size_t offset, size_t length, uint64_t name) final {
            assert(reinterpret_cast<size_t>(dest) % 4096 == 0);
            assert(offset % 4096 == 0);
            assert(length % 4096 == 0);
            assert(length > 0);
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                fprintf(stderr, "io_uring_get_sqe\n");
                exit(1);
            }

            sqe->opcode = IORING_OP_READ;
            sqe->fd = fd;
            sqe->off = offset;
            sqe->addr = reinterpret_cast<uint64_t>(dest);
            sqe->len = length;
            sqe->user_data = name;

            int ret = io_uring_submit(&ring);
            if (ret < 0) {
                fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
                exit(1);
            }
        }

        uint64_t awaitAny() final {
            struct io_uring_cqe *cqe = nullptr;
            int ret = io_uring_wait_cqe(&ring, &cqe);
            if (ret < 0) {
                fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
                exit(1);
            }
            uint64_t name = cqe->user_data;
            io_uring_cqe_seen(&ring, cqe);
            return name;
        }
};
#endif //HAS_LIBURING
