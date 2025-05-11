//
// Created by Ahmed Diab on 08.04.25.
//

#ifndef EXTERNAL_JOIN_BENCHMARKCOUNTER_H
#define EXTERNAL_JOIN_BENCHMARKCOUNTER_H

#include <chrono>
#include <cstdint>
#include <iostream>

namespace external_join {

    class JoinBenchmarkCounter {
    public:
        std::chrono::steady_clock::time_point build_start, build_end;
        std::chrono::steady_clock::time_point probe_start, probe_end;
        size_t result;

        void startBuild() {
            build_start = std::chrono::steady_clock::now();
        }

        void endBuild() {
            build_end = std::chrono::steady_clock::now();
        }

        void startProbe() {
            probe_start = std::chrono::steady_clock::now();
        }

        void endProbe() {
            probe_end = std::chrono::steady_clock::now();
        }

        uint64_t buildDurationMilliseconds() const {
            return std::chrono::duration_cast<std::chrono::milliseconds>(build_end - build_start).count();
        }

        uint64_t probeDurationMilliseconds() const {
            return std::chrono::duration_cast<std::chrono::milliseconds>(probe_end - probe_start).count();
        }


        void printResults() const {
            std::cout << "Build Phase Duration: " << buildDurationMilliseconds() << " ms" << std::endl;
            std::cout << "Probe Phase Duration: " << probeDurationMilliseconds() << " ms" << std::endl;
            std::cout << "Result: " << result << std::endl;
        }

    };
}

#endif //EXTERNAL_JOIN_BENCHMARKCOUNTER_H
