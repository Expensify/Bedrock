#pragma once
// If the environment sets VERSION_STRING, we'll use that value, otherwise we'll default to a bunch of zeros.
#ifdef VERSION
#error VERSION already defined
#endif
#ifdef VERSION_STRING
#define STRINGIZE(x) #x
#define EXPAND_STRING(x) STRINGIZE(x)
#define VERSION EXPAND_STRING(VERSION_STRING)
#else
#define VERSION "0000000"
#endif
