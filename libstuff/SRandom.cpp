#include "SRandom.h"

#include <thread>

#ifdef VALGRIND
// random_device breaks valgrind.
thread_local mt19937_64 SRandom::_generator = mt19937_64();
#else
// The thread id is mixed into the seed because random_device can hand the same 32-bit value to two threads that seed
// at the same moment, which would leave them generating identical sequences.
thread_local mt19937_64 SRandom::_generator = mt19937_64(random_device()() ^ hash<thread::id>()(this_thread::get_id()));
#endif

thread_local uniform_int_distribution<uint64_t> SRandom::_distribution64 = uniform_int_distribution<uint64_t>();

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
