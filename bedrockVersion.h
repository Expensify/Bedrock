#pragma once
// If the environment sets GIT_REVISION, we'll use that value, otherwise we'll default to a bunch of zeros.
#ifdef VERSION
#error VERSION already defined
#endif
#ifdef GIT_REVISION
#define STRINGIZE(x) #x
#define EXPAND_STRING(x) STRINGIZE(x)
#define VERSION EXPAND_STRING(GIT_REVISION)
#else
#define VERSION "0000000"
#endif
