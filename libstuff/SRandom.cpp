#include <libstuff/libstuff.h>

mt19937_64 SRandom::_generator = mt19937_64(random_device()());
uniform_int_distribution<uint64_t> SRandom::_distribution64 = uniform_int_distribution<uint64_t>();

uint64_t SRandom::rand64() { return _distribution64(_generator); }
