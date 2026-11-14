#pragma once

#include <random>
#include <string>

using namespace std;

// Non-cryptographic random number generator, allocated and seeded on first use in each thread.
class SRandom {
public:
    static uint64_t rand64();
    static uint64_t limitedRand64(uint64_t min, uint64_t max);
    static string randStr(unsigned length);
    static bool randBool(const double probability);

private:
    static mt19937_64& _getGenerator();
};
