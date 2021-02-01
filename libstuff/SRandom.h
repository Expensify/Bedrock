#pragma once

// Random number generator class.
class SRandom {
  public:
    static uint64_t rand64();
    static uint64_t limitedRand64(uint64_t min, uint64_t max);
    static string randStr(size_t length);

  private:
    static mt19937_64 _generator;
    static uniform_int_distribution<uint64_t> _distribution64;
};
