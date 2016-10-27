#pragma once
#include <random>
using namespace std;

// Random number generator class.
class SRandom {
private:
    static mt19937_64* generator;
    static uniform_int_distribution<uint64_t>* distribution64;
    static void init();
public:
    static uint64_t rand64();
};
