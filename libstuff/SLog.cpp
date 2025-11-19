#include "libstuff.h"
#include <execinfo.h> // for backtrace*

// Global logging state shared between all threads
atomic<int> _g_SLogMask(LOG_INFO);
atomic<bool> GLOBAL_IS_LIVE{true};

void SLogStackTrace(int level)
{
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

// If the param name is not in this whitelist, we will log <REDACTED> in addLogParams.
static set<string> PARAMS_WHITELIST = {
    "command",
    "commitElapsed",
    "Connection",
    "Content-Length",
    "count",
    "indexName",
    "isUnique",
    "logParam",
    "message",
    "peer",
    "prepareElapsed",
    "query",
    "readElapsed",
    "reason",
    "requestID",
    "rollbackElapsed",
    "rowNum",
    "status",
    "topic",
    "totalElapsed",
    "userID",
    "what",
    "writeElapsed",
};

string addLogParams(string&& message, const STable& params)
{
    if (params.empty()) {
        return message;
    }

    message += " ~~";
    for (const auto& [key, value] : params) {
        message += " ";
        string valueToLog = value;
        if (!SContains(PARAMS_WHITELIST, key)) {
            if (!GLOBAL_IS_LIVE) {
                STHROW("500 Log param " + key + " not in the whitelist, either do not log that or add it to PARAMS_WHITELIST if it's not sensitive");
            }
            valueToLog = "<REDACTED>";
        }
        message += key + ": '" + valueToLog + "'";
    }

    return message;
}

void SWhitelistLogParams(const set<string>& params)
{
    PARAMS_WHITELIST.insert(params.begin(), params.end());
}
