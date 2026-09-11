#pragma once

#include <random>
#include <string>

using namespace std;

// Random number generator class.
class SRandom {
public:
    static uint64_t rand64();
    static uint64_t limitedRand64(uint64_t min, uint64_t max);
    static string randStr(unsigned length);
    static bool randBool(const double probability);

private:
    // Both of these carry mutable state that every draw advances, so each thread gets its own copy. Sharing one
    // generator across threads is a data race that hands the same value to more than one caller.
    static thread_local mt19937_64 _generator;
    static thread_local uniform_int_distribution<uint64_t> _distribution64;
};
