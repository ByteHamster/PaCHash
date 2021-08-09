#ifndef TESTCOMPARISON_QUERYTIMER_H
#define TESTCOMPARISON_QUERYTIMER_H

class QueryTimer {
    public:
        size_t numQueries = 0;

        void print() {
            std::cout<<"Time per query: "<<(double)(timeFindBlock+timeFetchBlock+timeFindObject)/numQueries<<" ns ("
                     <<"determine blocks: "<<(double)timeFindBlock/numQueries<<" ns, "
                     <<"fetch blocks: "<<(double)timeFetchBlock/numQueries<<" ns, "
                     <<"find object: "<<(double)timeFindObject/numQueries<<" ns)"<<std::endl;
        }
    private:
        size_t timeFindBlock = 0;
        size_t timeFetchBlock = 0;
        size_t timeFindObject = 0;
        size_t state = 0;
        std::chrono::system_clock::time_point timepoints[4];

    public:
        void notifyStartQuery(size_t numSimultaneous) {
            numQueries += numSimultaneous;
            timepoints[0] = std::chrono::high_resolution_clock::now();
            assert(state++ == 0);
        }

        void notifyFoundBlock() {
            timepoints[1] = std::chrono::high_resolution_clock::now();
            assert(state++ == 1);
        }

        void notifyFetchedBlock() {
            timepoints[2] = std::chrono::high_resolution_clock::now();
            assert(state++ == 2);
        }

        void notifyFoundKey() {
            timepoints[3] = std::chrono::high_resolution_clock::now();
            timeFindBlock += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[1] - timepoints[0]).count();
            timeFetchBlock += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[2] - timepoints[1]).count();
            timeFindObject += std::chrono::duration_cast<std::chrono::nanoseconds>(timepoints[3] - timepoints[2]).count();
            assert(state == 3);
            state = 0;
        }
};

#endif //TESTCOMPARISON_QUERYTIMER_H
