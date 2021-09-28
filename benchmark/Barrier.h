#pragma once
#include <pthread.h>

class Barrier {
        pthread_barrier_t b_ = {};

    public:
        explicit Barrier(unsigned int num_threads) {
            int rc;
            rc = pthread_barrier_init(&b_, nullptr, num_threads);
            if (rc != 0) {
                throw std::system_error{rc, std::system_category(), "Failed to create barrier: "};
            }
        }

        Barrier(Barrier const &) = delete;
        Barrier &operator=(Barrier const &) = delete;

        template <typename Callable>
        void wait(Callable &&f) {
            int rc;
            rc = pthread_barrier_wait(&b_);
            if (rc == PTHREAD_BARRIER_SERIAL_THREAD) {
                f();
            } else if (rc != 0) {
                throw std::system_error{rc, std::system_category(), "Failed to wait for barrier: "};
            }
        }

        void wait() {
            int rc;
            rc = pthread_barrier_wait(&b_);
            if (rc != PTHREAD_BARRIER_SERIAL_THREAD && rc != 0) {
                throw std::system_error{rc, std::system_category(), "Failed to wait for barrier: "};
            }
        }

        ~Barrier() noexcept {
            int rc = pthread_barrier_destroy(&b_);
            if (rc != 0) {
                auto e = std::system_error{rc, std::system_category(), "Failed to destroy barrier: "};
                std::cerr << e.what() << '\n';
            }
        }
};
