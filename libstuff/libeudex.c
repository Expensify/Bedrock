#include "libeudex.h"
#include <string.h>

static const uint64_t PHONES[] = {
    0b00000000, // a
    0b01001000, // b
    0b00001100, // c
    0b00011000, // d
    0b00000000, // e
    0b01000100, // f
    0b00001000, // g
    0b00000100, // h
    0b00000001, // i
    0b00000101, // j
    0b00001001, // k
    0b10100000, // l
    0b00000010, // m
    0b00010010, // n
    0b00000000, // o
    0b01001001, // p
    0b10101000, // q
    0b10100001, // r
    0b00010100, // s
    0b00011101, // t
    0b00000001, // u
    0b01000101, // v
    0b00000000, // w
    0b10000100, // x
    0b00000001, // y
    0b10010100  // z
    };

static const uint64_t PHONES_C1[] = {
    0b00010101, // ß
    0b00000000, // à
    0b00000000, // á
    0b00000000, // â
    0b00000000, // ã
    0b00000000, // ä [æ]
    0b00000001, // å [oː]
    0b00000000, // æ [æ]
    0b10010101, // ç [t͡ʃ]
    0b00000001, // è
    0b00000001, // é
    0b00000001, // ê
    0b00000001, // ë
    0b00000001, // ì
    0b00000001, // í
    0b00000001, // î
    0b00000001, // ï
    0b00010101, // ð [ð̠] (represented as a non-plosive T)
    0b00010111, // ñ [nj] (represented as a combination of n and j)
    0b00000000, // ò
    0b00000000, // ó
    0b00000000, // ô
    0b00000000, // õ
    0b00000001, // ö [ø]
    0b11111111, // ÷
    0b00000001, // ø [ø]
    0b00000001, // ù
    0b00000001, // ú
    0b00000001, // û
    0b00000001, // ü
    0b00000001, // ý
    0b00010101, // þ [ð̠] (represented as a non-plosive T)
    0b00000001, // ÿ
    };

static const uint64_t INJECTIVE_PHONES[] = {
    0b10000100, // a*
    0b00100100, // b
    0b00000110, // c
    0b00001100, // d
    0b11011000, // e*
    0b00100010, // f
    0b00000100, // g
    0b00000010, // h
    0b11111000, // i*
    0b00000011, // j
    0b00000101, // k
    0b01010000, // l
    0b00000001, // m
    0b00001001, // n
    0b10010100, // o*
    0b00100101, // p
    0b01010100, // q
    0b01010001, // r
    0b00001010, // s
    0b00001110, // t
    0b11100000, // u*
    0b00100011, // v
    0b00000000, // w
    0b01000010, // x
    0b11100100, // y*
    0b01001010 // z
    };

static const uint64_t INJECTIVE_PHONES_C1[] = {
    0b00001011, // ß
    0b10000101, // à
    0b10000101, // á
    0b10000000, // â
    0b10000110, // ã
    0b10100110, // ä [æ]
    0b11000010, // å [oː]
    0b10100111, // æ [æ]
    0b01010100, // ç [t͡ʃ]
    0b11011001, // è
    0b11011001, // é
    0b11011001, // ê
    0b11000110, // ë [ə] or [œ]
    0b11111001, // ì
    0b11111001, // í
    0b11111001, // î
    0b11111001, // ï
    0b00001011, // ð [ð̠] (represented as a non-plosive T)
    0b00001011, // ñ [nj] (represented as a combination of n and j)
    0b10010101, // ò
    0b10010101, // ó
    0b10010101, // ô
    0b10010101, // õ
    0b11011100, // ö [œ] or [ø]
    0b11111111, // ÷
    0b11011101, // ø [œ] or [ø]
    0b11100001, // ù
    0b11100001, // ú
    0b11100001, // û
    0b11100101, // ü
    0b11100101, // ý
    0b00001011, // þ [ð̠] (represented as a non-plosive T)
    0b11100101 // ÿ
    };

static const int LETTERS = 26;
static const char SKIP = '0';

uint64_t distance(eudex_t a, eudex_t b);
uint64_t popcount64(eudex_t x);
uint64_t next_phonetic(eudex_t prev, char letter, char *result);
uint64_t first_phonetic(char letter);
int get_letter_index(char letter);

/* external functions */

eudex_t eudex_new(const char* input) {
    uint64_t first_byte = *input != '\0' ? first_phonetic(*input) : 0;
    ++input; // advance to next byte

    char n = 1; // limit to first 8 bytes of string (same as in rust impl)
    eudex_t res = 0L;
    while (1) {
        if ((n == 0) || (*input == '\0')) {
            break;
        }

        char r = '1';
        uint64_t phonetic = next_phonetic(res, *input, &r);
        if (r != SKIP) {
            res <<= 8;
            res |= phonetic;
            n <<= 1;
        }
        ++input;
    }

    return res | (first_byte << 56L);
}

eudex_t eudex_dist(const char* input1, const char* input2) {
    return distance(eudex_new(input1), eudex_new(input2));
}

/* internal implementation */

inline int get_letter_index(char letter) {
    return ((letter | 32) - 'a') & 0xFF;
}

uint64_t next_phonetic(eudex_t prev, char letter, char *result) {
    int index = get_letter_index(letter);
    uint64_t c = 0L;
    if (index < LETTERS) {
        c = PHONES[index];
    } else if (index >= 0xDF && index < 0xFF) {
        c = PHONES_C1[index - 0xDF];
    } else {
        *result = SKIP;
    }

    if ((c & 1) != (prev & 1)) {
        return c;
    }
    *result = SKIP;
    return c;
}

uint64_t first_phonetic(char letter) {
    int index = get_letter_index(letter);

    if (index < LETTERS) {
        return INJECTIVE_PHONES[index];
    }
    if (index >= 0xDF && index < 0xFF) {
        return INJECTIVE_PHONES_C1[index - 0xDF];
    }
    return 0;
}

uint64_t distance(eudex_t a, eudex_t b) {
    eudex_t dist = a ^ b;
    return popcount64(dist & 0xFF) + popcount64((dist >> 8) & 0xFF) * 2
        + popcount64((dist >> 16) & 0xFF) * 3
        + popcount64((dist >> 24) & 0xFF) * 5
        + popcount64((dist >> 32) & 0xFF) * 8
        + popcount64((dist >> 40) & 0xFF) * 13
        + popcount64((dist >> 48) & 0xFF) * 21
        + popcount64((dist >> 56) & 0xFF) * 34;
}

inline uint64_t popcount64(eudex_t x) {

#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcount(x);
#else
    const uint64_t m1 = 0x5555555555555555;
    const uint64_t m2 = 0x3333333333333333;
    const uint64_t m4 = 0x0f0f0f0f0f0f0f0f;
    const uint64_t h01 = 0x0101010101010101;
    x -= (x >> 1) & m1;
    x = (x & m2) + ((x >> 2) & m2);
    x = (x + (x >> 4)) & m4;
    return (x * h01) >> 56;
#endif

}

