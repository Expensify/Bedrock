#include "libstuff.h"
#include <execinfo.h> // for backtrace*

// Global logging state shared between all threads
atomic<int> _g_SLogMask(LOG_INFO);

void SLogStackTrace(int level) {
    // If the level isn't set in the log mask, nothing more to do
    if (!(_g_SLogMask & (1 << level))) {
        return;
    }
    // Output the symbols to the log
    void* callstack[100];
    int depth = backtrace(callstack, 100);
    vector<string> stack = SGetCallstack(depth, callstack);
    for (const auto& frame : stack) {
        switch (level) {
        case LOG_DEBUG:
            SDEBUG(frame);
            break;
        case LOG_INFO:
            SINFO(frame);
            break;
        case LOG_NOTICE:
            SHMMM(frame);
            break;
        case LOG_WARNING:
            SWARN(frame);
            break;
        case LOG_ALERT:
            SALERT(frame);
            break;
        case LOG_ERR:
            SERROR(frame);
            break;
        default:
            break;
        }
    }
}

string addLogParams(string&& message, const map<string, string>& params) {
    if (params.empty()) {
        return message;
    }

    message += " ~~ ";
    for (size_t i = 0; i < params.size(); ++i) {
        if (i > 0) {
            message += " ";
        }
        const auto& param = *next(params.begin(), i);
        message += param.first + ": '" + param.second + "'";
    }

    return message;
}
