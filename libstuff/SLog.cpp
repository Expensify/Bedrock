#include "libstuff.h"
#include <execinfo.h> // for backtrace*

// --------------------------------------------------------------------------
// Global logging state; unsynchronized but shared between all threads
int _g_SLogMask      = LOG_WARNING;
bool _g_SLogToSTDOUT = false;

// --------------------------------------------------------------------------
void SLogStackTrace()
{
    // Output the symbols to the log
    void* callstack[100];
    int depth      = backtrace(callstack, 100);
    char** symbols = backtrace_symbols(callstack, depth);
    for (int c = 0; c < depth; ++c) {
        SWARN(symbols[c]);
    }
}
