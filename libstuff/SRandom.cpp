#include <libstuff/libstuff.h>

mt19937_64 SRandom::_generator = mt19937_64(random_device()());
uniform_int_distribution<uint64_t> SRandom::_distribution64 = uniform_int_distribution<uint64_t>();

uint64_t SRandom::rand64() { return _distribution64(_generator); }

string SRandom::randStr(uint& length) {
    string str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    string newstr;
    size_t pos;
    while(newstr.size() != length) {
        pos = (size_t)(rand64() % (str.size() - 1));
        newstr += str.substr(pos,1);
    }
    return newstr;
}
