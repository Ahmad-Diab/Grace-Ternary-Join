#ifndef EXTERNAL_JOIN_BENCHMARK_H
#define EXTERNAL_JOIN_BENCHMARK_H
#include <iostream>
#include <utility>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <functional>
#include <numeric>
#include <fstream>
#include <sstream>
#include <iomanip>

#include "BenchmarkCounter.h"

namespace external_join {

    struct BenchmarkResult {
        std::string taskName;
        int dataSize;
        std::vector<double> times;
        JoinBenchmarkCounter resultBenchmark;

        double min_time() const { return *std::min_element(times.begin(), times.end()); }
        double max_time() const { return *std::max_element(times.begin(), times.end()); }
        double avg_time() const { return std::accumulate(times.begin(), times.end(), 0.0) / times.size(); }
    };

    class BenchmarkFramework {
    public:
        void addTask(const std::string& taskName, std::function<JoinBenchmarkCounter()>* func, int dataSize) {
            benchmarks[taskName] = {func, dataSize};
        }

        void runBenchmarks(int iterations = 1) {
            for (const auto& [name, benchmark] : benchmarks) {
                BenchmarkResult result{name, benchmark.dataSize, {}};

                for (int i = 0; i < iterations; ++i) {
                    auto start = std::chrono::high_resolution_clock::now();
                    JoinBenchmarkCounter output =(*benchmark.func)();
                    auto end = std::chrono::high_resolution_clock::now();

                    double duration = std::chrono::duration<double, std::milli>(end - start).count();
                    result.times.push_back(duration);

                    if (i == 0) {
                        result.resultBenchmark = output;
                    }
                }

                results.push_back(result);
            }

            printResults();
        }

    private:
        struct BenchmarkTask {
            std::function<JoinBenchmarkCounter ()>* func;
            int dataSize;
        };

        std::unordered_map<std::string, BenchmarkTask> benchmarks;
        std::vector<BenchmarkResult> results;

        void printResults() {
            std::cout << "\n=== Benchmark Results ===\n";
            for (const auto& res : results) {
                std::cout << "Task: " << res.taskName << "\n"
                          << "Dataset Size: " << res.dataSize << "\n"
                          << "Min Time: " << res.min_time() << " ms\n"
                          << "Max Time: " << res.max_time() << " ms\n"
                          << "Avg Time: " << res.avg_time() << " ms\n";
                res.resultBenchmark.printResults();
                std::cout << "---------------------------\n";
            }
        }

        static std::string generateTimestampFilename(std::string_view extension) {
            auto now = std::chrono::system_clock::now();
            auto time = std::chrono::system_clock::to_time_t(now);
            std::tm tm = *std::localtime(&time);

            std::ostringstream oss;
            oss << "benchmark_" << std::put_time(&tm, "%Y-%m-%d_%H-%M-%S") << extension;
            return oss.str();
        }
    };
}
#endif //EXTERNAL_JOIN_BENCHMARK_H
