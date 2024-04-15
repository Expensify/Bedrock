#include "libstuff.h"
#include <execinfo.h> // for backtrace*

// Global logging state shared between all threads
atomic<int> _g_SLogMask(LOG_INFO);

void SLogStackTrace(int level) {
    // Output the symbols to the log
    void* callstack[100];
    int depth = backtrace(callstack, 100);
    vector<string> stack = SGetCallstack(depth, callstack);
    for (const auto& frame : stack) {
        SLogAtLevel(level, frame);
    }
}
