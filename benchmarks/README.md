# Bedrock Benchmarks

This directory contains performance benchmarks for Bedrock components.

## Quick Start

To run all benchmarks:
```bash
cd /vagrant/Bedrock
make bench
./benchmarks/bench
```

To run specific benchmarks:
```bash
./benchmarks/bench -only SDeburrBench
```

## Writing New Benchmarks

It's easy to write new performance tests. Here's a simple example:

```cpp
#include "BenchmarkBase.h"

struct MyBench : tpunit::TestFixture, BenchmarkBase {
    MyBench() : tpunit::TestFixture(
        "MyBench",
        TEST(MyBench::benchMyFunction)
    ), BenchmarkBase("MyBench") {}

    void benchMyFunction() {
        const std::vector<std::string> inputs = {"test1", "test2", "test3"};
        
        auto us = runBench("MyFunction", inputs, 10000, [](const std::string& s) {
            // Your function to benchmark here
            return s.size();
        });
        
        ASSERT_GREATER_THAN(us, 0);
    }
} __MyBench;
```

## Framework Features

- **Easy setup**: Just inherit from `BenchmarkBase` and call `runBench()`
- **Automatic warmup**: Runs your function 100 times to warm up CPU caches
- **Throughput calculation**: Automatically calculates MB/s for string inputs
- **Compiler protection**: Prevents the compiler from optimizing away your work
- **Setup/teardown**: Override `setup()` and `teardown()` methods if needed

## Benchmark Structure

- `BenchmarkBase.h` - The micro-framework base class
- `SDeburrBench.cpp` - Benchmarks for the `SDeburr::deburr` function
- `ExampleBench.cpp` - Example showing how to use the framework
- `main.cpp` - Simple main function that runs all benchmarks

## Adding to Build

New benchmark files are automatically included in the build. Just create a `.cpp` file in this directory and it will be compiled into the `bench` binary.

## Output Format

Benchmarks print results in this format:
```
[BenchmarkName] TestName: inputs=N, iters=N, bytes=N, time_us=N, throughput_MBps=N.N
```

## Baseline Comparison

Compare performance against any git ref (branch, tag, or commit):

```bash
./benchmarks/bench --baseline main
./benchmarks/bench --baseline HEAD~1
./benchmarks/bench --baseline v1.0.0
```

This feature will:
1. Stash any uncommitted changes
2. Checkout the baseline ref and run benchmarks
3. Return to your branch and restore changes
4. Run current benchmarks
5. Display a comparison report with:
   - **Green** ✅ for improvements
   - **Red** ⚠️ for regressions
   - Percentage changes for each benchmark
   - Summary statistics

Example output:
```
=== Benchmark Comparison Report ===

Benchmark                 Baseline MB/s   Current MB/s    Change %         Status
----------------------------------------------------------------------------------
SDeburrBench::Latin1            49.71          48.73        -2.0%  ⚠️  REGRESSED
SDeburrBench::ShortASCII       94.82          96.10        +1.3%  ✅ IMPROVED

Summary: 1 improved, 1 regressed, 0 unchanged
```

This makes it easy to ensure your optimizations actually improve performance!