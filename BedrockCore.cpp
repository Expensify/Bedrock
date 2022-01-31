#include <libstuff/libstuff.h>
#include "BedrockCore.h"
#include "BedrockPlugin.h"
#include "BedrockServer.h"

const uint64_t BedrockCore::DEFAULT_TIMEOUT = 290'000; // 290 seconds, so clients can have a 5 minute timeout.
const uint64_t BedrockCore::DEFAULT_TIMEOUT_FORGET = 60'000 * 60; // 1 hour for `connection: forget` commands.
const uint64_t BedrockCore::DEFAULT_PROCESS_TIMEOUT = 30'000; // 30 seconds.

BedrockCore::BedrockCore(SQLite& db, const BedrockServer& server) :
SQLiteCore(db),
_server(server)
{ }

// RAII-style mechanism for automatically setting and unsetting query rewriting 
class AutoScopeRewrite {
  public:
    AutoScopeRewrite(bool enable, SQLite& db, bool (*handler)(int, const char*, string&)) : _enable(enable), _db(db), _handler(handler) {
        if (_enable) {
            _db.setRewriteHandler(_handler);
            _db.enableRewrite(true);
        }
    }
    ~AutoScopeRewrite() {
        if (_enable) {
            _db.setRewriteHandler(nullptr);
            _db.enableRewrite(false);
        }
    }
  private:
    bool _enable;
    SQLite& _db;
    bool (*_handler)(int, const char*, string&);
};

uint64_t BedrockCore::_getTimeout(const SData& request, bool isProcessing) {

    // Timeout is the default, unless explicitly supplied, or if Connection: forget is set.
    uint64_t timeout =  DEFAULT_TIMEOUT;
    if (request.isSet("timeout")) {
        timeout = request.calc("timeout");
    } else if (SIEquals(request["connection"], "forget")) {
        timeout = DEFAULT_TIMEOUT_FORGET;
    }

    // We also want to know the processTimeout, because we'll return early if we get stuck processing for too long.
    int64_t processTimeout = request.isSet("processTimeout") ? request.calc("processTimeout") : DEFAULT_PROCESS_TIMEOUT;

    // See when the command was scheduled to run. The timeout is from *this* start time, not from when the command
    // starts executing.
    try {
        int64_t adjustedTimeout = (int64_t)timeout - (int64_t)((STimeNow() - stoull(request["commandExecuteTime"])) / 1000);

        // If this is negative, we're *already* past the timeout, just return early.
        if (adjustedTimeout <= 0 || processTimeout <= 0) {
            SALERT("Command " << request.methodLine << " timed out after " << (timeout - adjustedTimeout) << "ms.");
            STHROW("555 Timeout");
        } else {
            // Otherwise, we can return the shorter of our two timeouts.
            return isProcessing ? min(adjustedTimeout, processTimeout) : adjustedTimeout;
        }
    } catch (const invalid_argument& e) {
        SWARN("Couldn't parse commandExecuteTime: " << request["commandExecuteTime"] << "'.");
    } catch (const out_of_range& e) {
        SWARN("Invalid commandExecuteTime: " << request["commandExecuteTime"] << "'.");
    }

    // This only happens if we hit the catch blocks above. Default to a low value.
    return isProcessing ? min(DEFAULT_TIMEOUT, DEFAULT_PROCESS_TIMEOUT) : DEFAULT_TIMEOUT;
}
bool BedrockCore::peekCommand(BedrockCommand& command) {
    AutoTimer timer(command, BedrockCommand::PEEK);
    // Convenience references to commonly used properties.
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;

    // We catch any exception and handle in `_handleCommandException`.
    try {
        SDEBUG("Peeking at '" << request.methodLine << "' with priority: " << command.priority);
        uint64_t timeout = _getTimeout(request, false);
        command.peekCount++;

        _db.startTiming(timeout * 1000);
        // We start a transaction in `peekCommand` because we want to support having atomic transactions from peek
        // through process. This allows for consistency through this two-phase process. I.e., anything checked in
        // peek is guaranteed to still be valid in process, because they're done together as one transaction.
        bool pluginPeeked = false;

        // Some plugins want to alert timeout errors themselves, and make them silent on bedrock.
        bool shouldSuppressTimeoutWarnings = false;

        try {
            if (!_db.beginConcurrentTransaction()) {
                STHROW("501 Failed to begin concurrent transaction");
            }

            // Make sure no writes happen while in peek command
            _db.read("PRAGMA query_only = true;");

            // Try each plugin, and go with the first one that says it succeeded.
            for (auto plugin : _server.plugins) {
                shouldSuppressTimeoutWarnings = plugin->shouldSuppressTimeoutWarnings();

                // Try to peek the command.
                if (plugin->peekCommand(_db, command)) {
                    SINFO("Plugin '" << plugin->getName() << "' peeked command '" << request.methodLine << "'");
                    command.peekedBy = plugin;
                    pluginPeeked = true;
                    break;
                }
            }

            // Peeking is over now, allow writes
            _db.read("PRAGMA query_only = false;");
        } catch (const SQLite::timeout_error& e) {
            if (!shouldSuppressTimeoutWarnings) {
                SALERT("Command " << command.request.methodLine << " timed out after " << e.time()/1000 << "ms.");
            }
            STHROW("555 Timeout peeking command");
        }

        // If nobody succeeded in peeking it, then we'll need to process it.
        // TODO: Would be nice to be able to check if a plugin *can* handle a command, so that we can differentiate
        // between "didn't peek" and "peeked but didn't complete".
        if (!pluginPeeked) {
            SINFO("Command '" << request.methodLine << "' is not peekable, queuing for processing.");
            _db.resetTiming();
            return false;
        }

        // If no response was set, assume 200 OK
        if (response.methodLine == "") {
            response.methodLine = "200 OK";
        }

        // Add the commitCount header to the response.
        response["commitCount"] = to_string(_db.getCommitCount());

        // Success. If a command has set "content", encode it in the response.
        SINFO("Responding '" << response.methodLine << "' to read-only '" << request.methodLine << "'.");
        if (!content.empty()) {
            // Make sure we're not overwriting anything different.
            string newContent = SComposeJSONObject(content);
            if (response.content != newContent) {
                if (!response.content.empty()) {
                    SWARN("Replacing existing response content in " << request.methodLine);
                }
                response.content = newContent;
            }
        }
    } catch (const SException& e) {
        _db.resetTiming();
        _db.read("PRAGMA query_only = false;");
        _handleCommandException(command, e);
    } catch (...) {
        _db.resetTiming();
        _db.read("PRAGMA query_only = false;");
        SALERT("Unhandled exception typename: " << SGetCurrentExceptionName() << ", command: " << request.methodLine);
        command.response.methodLine = "500 Unhandled Exception";
    }

    // If we get here, it means the command is fully completed.
    command.complete = true;

    // Back out of the current transaction, it doesn't need to do anything.
    _db.rollback();
    _db.resetTiming();

    // Done.
    return true;
}

bool BedrockCore::processCommand(BedrockCommand& command) {
    AutoTimer timer(command, BedrockCommand::PROCESS);

    // Convenience references to commonly used properties.
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;

    // Keep track of whether we've modified the database and need to perform a `commit`.
    bool needsCommit = false;
    try {
        SDEBUG("Processing '" << request.methodLine << "'");
        uint64_t timeout = _getTimeout(request, true);
        command.processCount++;

        // Time in US.
        _db.startTiming(timeout * 1000);
        // If a transaction was already begun in `peek`, then this is a no-op. We call it here to support the case where
        // peek created a httpsRequest and closed it's first transaction until the httpsRequest was complete, in which
        // case we need to open a new transaction.
        if (!_db.insideTransaction() && !_db.beginConcurrentTransaction()) {
            STHROW("501 Failed to begin concurrent transaction");
        }

        // Loop across the plugins to see which wants to take this.
        bool pluginProcessed = false;

        // If the command is mocked, turn on UpdateNoopMode.
        _db.setUpdateNoopMode(command.request.isSet("mockRequest"));
        for (auto plugin : _server.plugins) {
            // Try to process the command.
            bool (*handler)(int, const char*, string&) = nullptr;
            bool enable = plugin->shouldEnableQueryRewriting(_db, command, &handler);
            AutoScopeRewrite rewrite(enable, _db, handler);
            try {
                if (plugin->processCommand(_db, command)) {
                    SINFO("Plugin '" << plugin->getName() << "' processed command '" << request.methodLine << "'");
                    pluginProcessed = true;
                    command.processedBy = plugin;
                    break;
                }
            } catch (const SQLite::timeout_error& e) {
                SALERT("Command " << command.request.methodLine << " timed out after " << e.time()/1000 << "ms.");
                STHROW("555 Timeout processing command");
            }
        }

        // If no plugin processed it, respond accordingly.
        if (!pluginProcessed) {
            SWARN("Command '" << request.methodLine << "' does not exist.");
            STHROW("430 Unrecognized command");
        }

        // If we have no uncommitted query, just rollback the empty transaction. Otherwise, we need to commit.
        if (_db.getUncommittedQuery().empty()) {
            _db.rollback();
        } else {
            needsCommit = true;
        }

        // If no response was set, assume 200 OK
        if (response.methodLine == "") {
            response.methodLine = "200 OK";
        }

        // Success, this command will be committed.
        SINFO("Processed '" << response.methodLine << "' for '" << request.methodLine << "'.");

        // Finally, if a command has set "content", encode it in the response.
        if (!content.empty()) {
            // Make sure we're not overwriting anything different.
            string newContent = SComposeJSONObject(content);
            if (response.content != newContent) {
                if (!response.content.empty()) {
                    SWARN("Replacing existing response content in " << request.methodLine);
                }
                response.content = newContent;
            }
        }
    } catch (const SException& e) {
        _handleCommandException(command, e);
        _db.rollback();
        needsCommit = false;
    } catch(...) {
        SALERT("Unhandled exception typename: " << SGetCurrentExceptionName() << ", command: " << request.methodLine);
        command.response.methodLine = "500 Unhandled Exception";
        _db.rollback();
        needsCommit = false;
    }

    // We can turn this back off now, this is a noop if it's not turned on.
    _db.setUpdateNoopMode(false);

    // We can reset the timing info for the next command.
    _db.resetTiming();

    // Done, return whether or not we need the parent to commit our transaction.
    command.complete = !needsCommit;
    return needsCommit;
}

void BedrockCore::_handleCommandException(BedrockCommand& command, const SException& e) {
    const string& msg = "Error processing command '" + command.request.methodLine + "' (" + e.what() + "), ignoring.";
    if (SContains(e.what(), "_ALERT_")) {
        SALERT(msg);
    } else if (SContains(e.what(), "_WARN_")) {
        SWARN(msg);
    } else if (SContains(e.what(), "_HMMM_")) {
        SHMMM(msg);
    } else if (SStartsWith(e.what(), "50")) {
        SALERT(msg); // Alert on 500 level errors.
    } else {
        SINFO(msg);
    }

    // Set the response to the values from the exception, if set.
    if (!e.method.empty()) {
        command.response.methodLine = e.method;
    }
    if (!e.headers.empty()) {
        command.response.nameValueMap = e.headers;
    }
    if (!e.body.empty()) {
        command.response.content = e.body;
    }

    // Add the commitCount header to the response.
    command.response["commitCount"] = to_string(_db.getCommitCount());
}
