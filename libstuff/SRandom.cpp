#include "SRandom.h"

#include <atomic>
#include <memory>

mt19937_64& SRandom::_getGenerator()
{
    static thread_local auto generator = [] {
#ifdef VALGRIND
        // random_device breaks valgrind; use distinct seeds so fresh threads do not repeat the same stream.
        static atomic<uint64_t> nextSeed{mt19937_64::default_seed};
        return make_unique<mt19937_64>(nextSeed.fetch_add(1, memory_order_relaxed));
#else
        random_device rd;
        seed_seq seed{rd(), rd(), rd(), rd(), rd(), rd(), rd(), rd()};
        return make_unique<mt19937_64>(seed);
#endif
    }();
    return *generator;
}

uint64_t SRandom::limitedRand64(uint64_t minNum, uint64_t maxNum)
{
    uniform_int_distribution<uint64_t> limitedRandom(minNum, maxNum);
    return limitedRandom(_getGenerator());
}

uint64_t SRandom::rand64()
{
    return _getGenerator()();
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
    return bernoulli_distribution(probability)(_getGenerator());
}
