#pragma once

#define MEASURE_QUERY_TIMING

namespace pacthash {
class QueryTimer {
    private:
        size_t timeFindBlock = 0;
        size_t timeFetchBlock = 0;
        size_t timeFindObject = 0;
        size_t state = 0;
        std::chrono::system_clock::time_point timepoints[4];

    public:
        size_t blocksFetched = 0;
        size_t numQueries = 0;

        friend auto operator<<(std::ostream& os, QueryTimer const& q) -> std::ostream& {
            os   << " determine_blocks=" << (double)q.timeFindBlock/q.numQueries
                 << " io_latency=" << (double)q.timeFetchBlock/q.numQueries
                 << " find_object=" << (double)q.timeFindObject/q.numQueries
                 << " blocks_fetched=" << (double)q.blocksFetched/q.numQueries;
            return os;
        }

        QueryTimer& operator+=(const QueryTimer& rhs){
            this->numQueries += rhs.numQueries;
            this->timeFindBlock += rhs.timeFindBlock;
            this->timeFetchBlock += rhs.timeFetchBlock;
            this->timeFindObject += rhs.timeFindObject;
            this->blocksFetched += rhs.blocksFetched;
            return *this;
        }

        QueryTimer& operator/=(const size_t& rhs){
            this->numQueries /= rhs;
            this->timeFindBlock /= rhs;
            this->timeFetchBlock /= rhs;
            this->timeFindObject /= rhs;
            this->blocksFetched /= rhs;
            return *this;
        }

        void notifyStartQuery() {
            numQueries++;
            #ifdef MEASURE_QUERY_TIMING
            timepoints[0] = std::chrono::high_resolution_clock::now();
            assert(state++ == 0);
            #endif
        }

        void notifyFoundBlock(size_t numBlocks) {
            blocksFetched += numBlocks;
            #ifdef MEASURE_QUERY_TIMING
            timepoints[1] = std::chrono::high_resolution_clock::now();
            assert(state++ == 1);
            #endif
        }

        void notifyFetchedBlock() {
            #ifdef MEASURE_QUERY_TIMING
            timepoints[2] = std::chrono::high_resolution_clock::now();
            assert(state++ == 2);
            #endif
        }

        void notifyFoundKey() {
            #ifdef MEASURE_QUERY_TIMING
            timepoints[3] = std::chrono::high_resolution_clock::now();
            timeFindBlock += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[1] - timepoints[0]).count();
            timeFetchBlock += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[2] - timepoints[1]).count();
            timeFindObject += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[3] - timepoints[2]).count();
            assert(state == 3);
            state = 0;
            #endif
        }
};

} // Namespace pacthash
