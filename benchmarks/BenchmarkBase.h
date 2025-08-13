#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <chrono>
#include <libstuff/libstuff.h>
#include <test/lib/tpunit++.hpp>

using namespace std;

// Global storage for benchmark results
struct BenchmarkResult {
    double throughputMBps;
    uint64_t elapsedUs;
    size_t totalBytes;
};

extern map<string, BenchmarkResult> g_benchmarkResults;

/**
 * Simple micro-framework for writing performance benchmarks.
 * 
 * To create a new benchmark:
 * 1. Inherit from BenchmarkBase
 * 2. Override setup() and/or teardown() if needed
 * 3. Call runBench() with your test data and function
 * 
 * Example:
 *   void myBenchmark() {
 *       vector<string> inputs = {"test1", "test2"};
 *       runBench("MyTest", inputs, 10000, [](const string& s) {
 *           return s.size(); // Your actual work here
 *       });
 *   }
 */
class BenchmarkBase {
public:
    BenchmarkBase(const string& name) : testName(name) {}

private:
    string testName;

protected:
    /**
     * Override this to set up data before benchmarks run.
     */
    virtual void setup() {}

    /**
     * Override this to clean up after benchmarks finish.
     */
    virtual void teardown() {}

    /**
     * Run a benchmark with the given inputs and function.
     * 
     * @param name Human-readable name for this benchmark
     * @param inputs Test data to run the function on
     * @param iterations How many times to run each input
     * @param func The function to benchmark (takes input, returns anything)
     * @return Elapsed time in microseconds
     */
    template<typename InputType, typename Func>
    uint64_t runBench(const string& name,
                      const vector<InputType>& inputs,
                      int iterations, 
                      Func func) {
        setup();

        // Warm-up: run a few times to get CPU caches ready
        volatile size_t guard = 0;
        for (int i = 0; i < 100; ++i) {
            for (const auto& input : inputs) {
                auto result = func(input);
                guard += sizeof(result); // Use the result somehow
            }
        }

        // Actual timing
        const auto start = chrono::high_resolution_clock::now();
        size_t totalBytes = 0

        for (int it = 0; it < iterations; ++it) {
            for (const auto& input : inputs) {
                auto result = func(input);
                guard += sizeof(result); // Prevent optimization
                totalBytes += getInputSize(input);
            }
        }

        const auto end = chrono::high_resolution_clock::now();
        const uint64_t elapsedUs = chrono::duration_cast<chrono::microseconds>(end - start).count();

        // Calculate and print throughput
        const double seconds = static_cast<double>(elapsedUs) / 1'000'000.0;
        const double mbProcessed = static_cast<double>(totalBytes) / (1024.0 * 1024.0);
        const double mbps = seconds > 0.0 ? (mbProcessed / seconds) : 0.0;

        // Store result for comparison
        string fullName = testName + "::" + name;
        g_benchmarkResults[fullName] = {mbps, elapsedUs, totalBytes};

        cout << "[" << testName << "] " << name
                  << ": inputs=" << inputs.size()
                  << ", iters=" << iterations
                  << ", bytes=" << totalBytes
                  << ", time_us=" << elapsedUs
                  << ", throughput_MBps=" << mbps
                  << endl;

        // Ensure the compiler doesn't optimize away our work
        if (guard == 0) {
            cout << "[" << testName << "] guard==0" << endl;
        }

        teardown();
        return elapsedUs;
    }

private:
    /**
     * Get the size in bytes of different input types.
     */
    template<typename T>
    size_t getInputSize(const T& input) {
        if constexpr (is_same_v<T, string>) {
            return input.size();
        } else {
            return sizeof(T);
        }
    }
};
