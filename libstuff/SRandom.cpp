#include "SRandom.h"

#ifdef VALGRIND
// random_device breaks valgrind.
mt19937_64 SRandom::_generator = mt19937_64();
#else
mt19937_64 SRandom::_generator = mt19937_64(random_device()());
#endif

uniform_int_distribution<uint64_t> SRandom::_distribution64 = uniform_int_distribution<uint64_t>();

uint64_t SRandom::limitedRand64(uint64_t minNum, uint64_t maxNum) {
     uniform_int_distribution<uint64_t> limitedRandom(minNum, maxNum);
     return limitedRandom(_generator);
}

uint64_t SRandom::rand64() {
    return _distribution64(_generator);
}

string SRandom::randStr(uint length) {
    string str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    string newstr;
    int pos;
    while (newstr.size() != length) {
        pos = (rand64() % (str.size() - 1));
        newstr += str.substr(pos, 1);
    }
    return newstr;
}
