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
g