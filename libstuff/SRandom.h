#pragma once

// Random number generator class.
class SRandom {
  public:
    static uint64_t rand64();
    static string randStr(uint& length);

  private:
    static mt19937_64 _generator;
    static uniform_int_distribution<uint64_t> _distribution64;
};
