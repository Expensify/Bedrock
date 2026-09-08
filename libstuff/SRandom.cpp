/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SRandom.cpp
 * Path:    libstuff/SRandom.cpp
 * Pair:    SRandom.h
 *
 * INTENT
 *   Implements SRandom; see the header for the public contract.
 *
 * OBJECTS
 *   SRandom::_generator     - static mt19937_64, seeded from random_device
 *                             (or deterministically under VALGRIND, since
 *                             random_device breaks valgrind).
 *   SRandom::_distribution64 - static full-range uint64_t distribution.
 *   SRandom::rand64/limitedRand64/randStr/randBool - implement the
 *                             header's declared static methods.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Fits repo convention.
 * ─────────────────────────────────────────────────────────────────────*/
#include "SRandom.h"

#ifdef VALGRIND
// random_device breaks valgrind.
mt19937_64 SRandom::_generator = mt19937_64();
#else
mt19937_64 SRandom::_generator = mt19937_64(random_device()());
#endif

uniform_int_distribution<uint64_t> SRandom::_distribution64 = uniform_int_distribution<uint64_t>();

uint64_t SRandom::limitedRand64(uint64_t minNum, uint64_t maxNum)
{
    uniform_int_distribution<uint64_t> limitedRandom(minNum, maxNum);
    return limitedRandom(_generator);
}

uint64_t SRandom::rand64()
{
    return _distribution64(_generator);
}

string SRandom::randStr(unsigned length)
{
    string str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    string newstr;
    int pos;
    while (newstr.size() != length) {
        pos = (rand64() % (str.size() - 1));
        newstr += str.substr(pos, 1);
    }
    return newstr;
}

bool SRandom::randBool(const double probability)
{
    return bernoulli_distribution(probability)(_generator);
}
