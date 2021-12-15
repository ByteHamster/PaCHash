#pragma once

namespace pachash {
class ConstructionTimer {
    public:
        friend auto operator<<(std::ostream& os, ConstructionTimer const& q) -> std::ostream& {
            os << " total_construction=" << (double)(q.timeDetermineSize+q.timePlaceObjects
                                                        +q.timeWriteObjects+q.timeReadFromFile)
                 << " determine_size=" << (double)q.timeDetermineSize
                 << " place_objects=" << (double)q.timePlaceObjects
                 << " write_objects=" << (double)q.timeWriteObjects
                 << " sync_file=" << (double)q.timeSyncFile
                 << " read_objects=" << (double)q.timeReadFromFile;
            return os;
        }
    private:
        size_t timeDetermineSize = 0;
        size_t timePlaceObjects = 0;
        size_t timeWriteObjects = 0;
        size_t timeSyncFile = 0;
        size_t timeReadFromFile = 0;
        size_t state = 0;
        std::chrono::system_clock::time_point timepoints[6];

    public:
        void notifyStartConstruction() {
            timepoints[0] = std::chrono::high_resolution_clock::now();
            assert(state++ == 0);
        }

        void notifyDeterminedSpace() {
            timepoints[1] = std::chrono::high_resolution_clock::now();
            assert(state++ == 1);
        }

        void notifyPlacedObjects() {
            timepoints[2] = std::chrono::high_resolution_clock::now();
            assert(state++ == 2);
        }

        void notifyWroteObjects() {
            timepoints[3] = std::chrono::high_resolution_clock::now();
            assert(state++ == 3);
        }

        void notifySyncedFile() {
            timepoints[4] = std::chrono::high_resolution_clock::now();
            assert(state == 0 || state++ == 4);
        }

        void notifyReadComplete() {
            timepoints[5] = std::chrono::high_resolution_clock::now();
            timeDetermineSize += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[1] - timepoints[0]).count();
            timePlaceObjects += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[2] - timepoints[1]).count();
            timeWriteObjects += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[3] - timepoints[2]).count();
            timeSyncFile += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[4] - timepoints[3]).count();
            timeReadFromFile += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[5] - timepoints[4]).count();
            assert(state == 0 || state == 5);
            state = 0;
        }
};

} // Namespace pachash
