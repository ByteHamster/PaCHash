#include <liburing.h>
#include <cassert>
#include <random>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <tlx/cmdline_parser.hpp>

#define DEPTH 128
#define BATCH_COMPLETE 32
#define BS 4096

int fd;
char *buffer;
struct io_uring ring = {};
unsigned long blocks = 0;

void prep_one(int index) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    assert(sqe != nullptr);
    io_uring_prep_read(sqe, fd, buffer + index * BS, BS, (lrand48() % blocks)*4096);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void *>(index));
}

unsigned reap_events() {
    unsigned completed = 0;
    struct io_uring_cqe *cqe = nullptr;
    while (true) {
        io_uring_peek_cqe(&ring, &cqe);
        if (cqe == nullptr) {
            return completed;
        }
        assert(cqe->res == BS);
        int index = static_cast<int>(reinterpret_cast<long int>(io_uring_cqe_get_data(cqe)));
        io_uring_cqe_seen(&ring, cqe);
        prep_one(index);
        completed++;
    }
    return -1;
}

int main(int argc, char** argv) {
    std::string path = "/dev/nvme1n1";
    size_t maxSize = -1;

    tlx::CmdlineParser cmd;
    cmd.add_string('f', "filename", path, "File to read");
    cmd.add_size_t('s', "max_size", maxSize, "Maximum file size to read, in GB");
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

    buffer = static_cast<char *>(aligned_alloc(BS, DEPTH * BS));
    int ret = io_uring_queue_init(DEPTH, &ring, IORING_SETUP_IOPOLL);
    assert(ret == 0);
    for (int i = 0; i < DEPTH; i++) {
        prep_one(i);
    }
    ret = io_uring_submit(&ring);
    assert(ret >= 0);

    struct timeval begin = {};
    struct timeval end = {};
    gettimeofday(&begin, nullptr);

    int done = 0;
    while (done < 1000000) {
        unsigned reaped = reap_events();
        ret = io_uring_submit_and_wait(&ring, BATCH_COMPLETE);
        assert(ret == reaped);
        done += ret;
    }

    gettimeofday(&end, nullptr);
    long seconds = end.tv_sec - begin.tv_sec;
    long microseconds = end.tv_usec - begin.tv_usec;
    double elapsed = seconds + microseconds * 1e-6;
    unsigned long iops = 1.0 / elapsed * done;
    printf("RESULT blocks=%lu iops=%lu\n", blocks, iops);
}
