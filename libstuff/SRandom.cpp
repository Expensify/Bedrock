#include "SRandom.h"

mt19937_64* SRandom::generator = 0;
uniform_int_distribution<uint64_t>* SRandom::distribution64 = 0;

void SRandom::init(){
    if (!generator) {
        random_device randomDevice;
        generator = new mt19937_64(randomDevice());
    }
    if (!distribution64) {
        distribution64 = new uniform_int_distribution<uint64_t>;
    }
}

uint64_t SRandom::rand64(){
    init();
    return (*distribution64)(*generator);
}

