#ifndef LIB_EUDEX_H
#define LIB_EUDEX_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t eudex_t;

extern eudex_t eudex_new(const char* input);
extern eudex_t eudex_dist(const char* input1, const char* input2);

#ifdef __cplusplus
}
#endif

#endif
