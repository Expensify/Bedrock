#include "libstuff.h"
#include <execinfo.h> // for backtrace*

// --------------------------------------------------------------------------
// Global logging state; unsynchronized but shared between all threads
int _g_SLogMask = LOG_WARNING;

// --------------------------------------------------------------------------
void SLogStackTrace() {
    // Output the symbols to the log
    void* callstack[100];
    int depth = backtrace(callstack, 100);
    vector<string> stack = SGetCallstack(depth, callstack);
    for (const auto& frame : stack) {
        SWARN(frame);
    }
}
