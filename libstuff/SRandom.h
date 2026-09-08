/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SRandom.h
 * Path:    libstuff/SRandom.h
 * Pair:    SRandom.cpp
 *
 * INTENT
 *   Static-only wrapper around a shared mt19937_64 generator, providing
 *   the repo's common random-number needs: raw 64-bit values, a bounded
 *   range, a random alphanumeric string, and a weighted boolean.
 *
 * OBJECTS
 *   SRandom  - all-static utility class; no instances. rand64/limitedRand64
 *              draw integers, randStr builds an alphanumeric string,
 *              randBool does a Bernoulli trial. Holds the shared generator
 *              and distribution as static state.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Fits repo convention (S-prefix, leading-underscore statics).
 * ─────────────────────────────────────────────────────────────────────*/
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
    static mt19937_64 _generator;
    static uniform_int_distribution<uint64_t> _distribution64;
};
