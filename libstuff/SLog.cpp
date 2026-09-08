/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SLog.cpp
 * Path:    libstuff/SLog.cpp
 * Pair:    (declarations in libstuff/libstuff.h)
 *
 * INTENT
 *   Global logging support: the process-wide log-level mask, stack-trace dumping for
 *   the SLOG macros, and a copy-on-write parameter whitelist that redacts any
 *   structured log param (`{{key, value}}`) not explicitly allowed, so arbitrary
 *   caller data can't leak into logs unreviewed.
 *
 * OBJECTS
 *   _g_SLogMask       - global atomic log-level bitmask read by the SLOG macros (declared extern in libstuff.h).
 *   GLOBAL_IS_LIVE    - global atomic flag; when false (non-production), an unwhitelisted
 *     log param throws instead of being silently redacted, to catch the omission in dev/test.
 *   SLogStackTrace()  - dumps the current call stack at a given log level.
 *   PARAMS_WHITELIST  - file-local static atomic<shared_ptr<const set<string>>>, the
 *     allowed structured-log-param names; swapped via CAS so readers stay lock-free.
 *   addLogParams()    - appends whitelisted (or command-tagged) params to a log message,
 *     redacting/throwing on anything not in PARAMS_WHITELIST.
 *   SWhitelistLogParams() / SIsLogParamWhitelisted() - mutate/query PARAMS_WHITELIST.
 *
 * OUT OF PLACE
 *   Nothing - this is what libstuff.h's forward-declared logging globals are the
 *   implementation of; the file matches its role, just not its own header.
 *
 * NAME/LOCATION FIT
 *   Name fits the content (logging internals). [CANDIDATE] Declared in the libstuff.h
 *   catch-all rather than a dedicated SLog.h, so callers can't see this file's
 *   contract without reading libstuff.h in full.
 *
 * NAMING QUALITY
 *   `_g_SLogMask` mixes a `_g` (global) marker with the `S`-prefix convention, unlike
 *   `GLOBAL_IS_LIVE`, which spells "global" out in full - two different conventions
 *   for the same idea (a process-wide global) in one file.
 * ─────────────────────────────────────────────────────────────────────*/

#include "libstuff.h"
#include <execinfo.h> // for backtrace*
#include <memory>

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
// Held as an immutable snapshot so readers (every parameterized log call) are lock-free;
// SWhitelistLogParams swaps in a new snapshot via copy-on-write CAS.
static atomic<shared_ptr<const set<string>>> PARAMS_WHITELIST{
    make_shared<const set<string>>(set<string>{
        "beginElapsed",
        "blockDurationMS",
        "chatID",
        "command",
        "commitElapsed",
        "commitLockElapsed",
        "Connection",
        "Content-Length",
        "count",
        "dimension",
        "hctstats",
        "identifier",
        "indexName",
        "isUnique",
        "logParam",
        "message",
        "newPriority",
        "oldPriority",
        "peer",
        "prepareElapsed",
        "query",
        "readElapsed",
        "reason",
        "requestID",
        "rollbackElapsed",
        "rowNum",
        "status",
        "timeMS",
        "thresholdMS",
        "topic",
        "totalElapsed",
        "totalTransactionElapsed",
        "userID",
        "what",
        "writeElapsed",
    })
};

string addLogParams(string&& message, const STable& params)
{
    // Every log line emitted while a command is running gets tagged with that command's name, unless the call
    // site already passed its own `command` param (e.g. logging about a *different* command than the one
    // currently executing on this thread).
    const bool addCommand = !SThreadLogCommand.empty() && !params.count("command");

    if (params.empty() && !addCommand) {
        return message;
    }

    message += " ~~";
    auto whitelist = PARAMS_WHITELIST.load();
    for (const auto& [key, value] : params) {
        message += " ";
        string valueToLog = value;
        if (!SContains(*whitelist, key)) {
            if (!GLOBAL_IS_LIVE) {
                STHROW("500 Log param " + key + " not in the whitelist, either do not log that or add it to PARAMS_WHITELIST if it's not sensitive");
            }
            valueToLog = "<REDACTED>";
        }
        message += key + ": '" + valueToLog + "'";
    }

    if (addCommand) {
        message += " command: '" + SThreadLogCommand + "'";
    }

    return message;
}

void SWhitelistLogParams(const set<string>& params)
{
    auto current = PARAMS_WHITELIST.load();
    while (true) {
        auto next = make_shared<set<string>>(*current);
        next->insert(params.begin(), params.end());
        if (PARAMS_WHITELIST.compare_exchange_strong(current, next)) {
            return;
        }
    }
}

bool SIsLogParamWhitelisted(const string& key)
{
    return SContains(*PARAMS_WHITELIST.load(), key);
}
