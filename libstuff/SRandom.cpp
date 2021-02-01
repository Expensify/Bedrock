#include <libstuff/libstuff.h>

mt19937_64 SRandom::_generator = mt19937_64(random_device()());
uniform_int_distribution<uint64_t> SRandom::_distribution64 = uniform_int_distribution<uint64_t>();

uint64_t SRandom::limitedRand64(uint64_t minNum, uint64_t maxNum) {
     uniform_int_distribution<uint64_t> limitedRandom(minNum, maxNum);
     return limitedRandom(_generator);
}

uint64_t SRandom::rand64() {
    return _distribution64(_generator);
}

string SRandom::randStr(size_t length) {
    char randomChars[63] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    char newstr[length + 1] = {0};
    uniform_int_distribution<char> limitedRandom(0, 61);
    for (size_t i = 0; i < length; i++) {
        newstr[i] = randomChars[(size_t)limitedRandom(_generator)];
    }

    return newstr;
}
