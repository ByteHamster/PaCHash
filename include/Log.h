#pragma once

#include <iostream>
#include <cmath>

#include "StoreConfig.h"

namespace pachash {
inline static void LOG(const char *step, size_t progress = ~0ul, size_t max = ~0) {
    #ifdef LOGGING_ENABLED
        constexpr int PROGRESS_STEPS = 16; // Power of 2 for faster division
        if (step == nullptr) [[unlikely]] {
            std::cout << "\r\033[K" << std::flush;
        } else if (progress == ~0ul) [[unlikely]] {
            std::cout << "\r\033[K# " << step << std::flush;
        } else if ((progress % (max / PROGRESS_STEPS + 1)) == 0 || progress == max - 1) [[unlikely]] {
            std::cout << "\r\033[K# " << step
                      << " (" << std::round(100.0 * (double) progress / (double) max) << "%)" << std::flush;
        }
    #endif
}
} // Namespace pachash
