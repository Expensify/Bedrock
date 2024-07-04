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

  private:
    static mt19937_64 _generator;
    static uniform_int_distribution<uint64_t> _distribution64;
};
