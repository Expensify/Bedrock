#include "libstuff.h"
#include <execinfo.h> // for backtrace*

#include <iostream>

// Global logging state shared between all threads
atomic<int> _g_SLogMask(LOG_INFO);

void SLogStackTrace() {
    // Output the symbols to the log
    void* callstack[100];
    int depth = backtrace(callstack, 100);
    vector<string> stack = SGetCallstack(depth, callstack);
    for (const auto& frame : stack) {
        cout << (frame) << endl;
    }
}
