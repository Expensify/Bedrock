#include <libstuff/libstuff.h>
#include "BedrockCore.h"
#include "BedrockPlugin.h"
#include "BedrockServer.h"

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

uint64_t BedrockCore::_getRemainingTime(const unique_ptr<BedrockCommand>& command, bool isProcessing) {
    int64_t timeout = command->timeout();
    int64_t now = STimeNow();

    // This is what's left for the "absolute" time. If it's negative, we've already timed out.
    int64_t adjustedTimeout = timeout - now;

    // We also want to know the processTimeout, because we'll return early if we get stuck processing for too long.
    int64_t processTimeout = command->request.isSet("processTimeout") ? command->request.calc("processTimeout") : BedrockCommand::DEFAULT_PROCESS_TIMEOUT;

    // Since timeouts are specified in ms, we convert to us.
    processTimeout *= 1000;

    // Already expired.
    if (adjustedTimeout <= 0 || (isProcessing && processTimeout <= 0)) {
        SALERT("Command " << command->request.methodLine << " timed out.");
        STHROW("555 Timeout");
    }

    // Both of these are positive, return the lowest remaining.
    return isProcessing ? min(processTimeout, adjustedTimeout) : adjustedTimeout;
}

bool BedrockCore::isTimedOut(unique_ptr<BedrockCommand>& command, SQLite* db, const BedrockServer* server) {
    try {
        _getRemainingTime(command, false);
    } catch (const SException& e) {
        // Yep, timed out.
        _handleCommandException(command, e, db, server);
        command->complete = true;
        return true;
    }
    return false;
}

void BedrockCore::prePeekCommand(unique_ptr<BedrockCommand>& command, bool isBlockingCommitThread) {
    AutoTimer timer(command, isBlockingCommitThread ? BedrockCommand::BLOCKING_PREPEEK : BedrockCommand::PREPEEK);

    // Convenience references to commonly used properties.
    const SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;

    try {
        try {
            SDEBUG("prePeeking at '" << request.methodLine << "' with priority: " << command->priority);
            command->prePeekCount++;
            _db.setTimeout(_getRemainingTime(command, false));

            // Make sure no writes happen while in prePeek command
            _db.setQueryOnly(true);

            // prePeek.
            command->reset(BedrockCommand::STAGE::PREPEEK);
            command->prePeek(_db);
            SDEBUG("Plugin '" << command->getName() << "' prePeeked command '" << request.methodLine << "'");

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
        } catch (const SQLite::timeout_error& e) {
            // Some plugins want to alert timeout errors themselves, and make them silent on bedrock.
            if (!command->shouldSuppressTimeoutWarnings()) {
                SALERT("Command " << command->request.methodLine << " timed out after " << e.time() / 1000 << "ms.");
            }
            STHROW("555 Timeout prePeeking command");
        }
    } catch (const SException& e) {
        _handleCommandException(command, e, &_db, &_server);
        command->complete = true;
    } catch (...) {
        SALERT("Unhandled exception typename: " << SGetCurrentExceptionName() << ", command: " << request.methodLine);
        command->response.methodLine = "500 Unhandled Exception";
        command->complete = true;
    }
    _db.clearTimeout();

    // Reset, we can write now.
    _db.setQueryOnly(false);
}

BedrockCore::RESULT BedrockCore::peekCommand(unique_ptr<BedrockCommand>& command, bool exclusive) {

    // Convenience references to commonly used properties.
    const SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;

    // We catch any exception and handle in `_handleCommandException`.
    RESULT returnValue = RESULT::COMPLETE;
    try {
        SDEBUG("Peeking at '" << request.methodLine << "' with priority: " << command->priority);
        command->peekCount++;
        _db.setTimeout(_getRemainingTime(command, false));

        try {
            if (!_db.beginTransaction(exclusive ? SQLite::TRANSACTION_TYPE::EXCLUSIVE : SQLite::TRANSACTION_TYPE::SHARED)) {
                STHROW("501 Failed to begin " + (exclusive ? "exclusive"s : "shared"s) + " transaction");
            }
            if (exclusive && command->writeConsistency != SQLiteNode::QUORUM) {
                decreaseCommandTimeout(command, BedrockCommand::DEFAULT_BLOCKING_TRANSACTION_COMMIT_LOCK_TIMEOUT);
            }

            // We start the timer here to avoid including the time spent acquiring the lock _sharedData.commitLock
            AutoTimer timer(command, exclusive ? BedrockCommand::BLOCKING_PEEK : BedrockCommand::PEEK);

            // Make sure no writes happen while in peek command
            _db.setQueryOnly(true);

            // Peek.
            command->reset(BedrockCommand::STAGE::PEEK);
            bool completed = command->peek(_db);
            SDEBUG("Plugin '" << command->getName() << "' peeked command '" << request.methodLine << "'");

            if (!completed) {
                SDEBUG("Command '" << request.methodLine << "' not finished in peek, re-queuing.");
                _db.clearTimeout();
                _db.setQueryOnly(false);
                return RESULT::SHOULD_PROCESS;
            }

        } catch (const SQLite::timeout_error& e) {
            // Some plugins want to alert timeout errors themselves, and make them silent on bedrock.
            if (!command->shouldSuppressTimeoutWarnings()) {
                SALERT("Command " << command->request.methodLine << " timed out after " << e.time()/1000 << "ms.");
            }
            STHROW("555 Timeout peeking command");
        }

        // If no response was set, assume 200 OK
        if (response.methodLine == "") {
            response.methodLine = "200 OK";
        }

        // Add the commitCount header to the response.
        response["commitCount"] = to_string(_db.getCommitCount());

        // Success. If a command has set "content", encode it in the response.
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
        command->repeek = false;
        _handleCommandException(command, e, &_db, &_server);
    } catch (const SHTTPSManager::NotLeading& e) {
        command->repeek = false;
        returnValue = RESULT::SHOULD_PROCESS;
        SINFO("Command '" << request.methodLine << "' wants to make HTTPS request, queuing for processing.");
    } catch (...) {
        command->repeek = false;
        SALERT("Unhandled exception typename: " << SGetCurrentExceptionName() << ", command: " << request.methodLine);
        command->response.methodLine = "500 Unhandled Exception";
    }

    // Unless an exception handler set this to something different, the command is complete.
    command->complete = returnValue == RESULT::COMPLETE;

    // Back out of the current transaction, it doesn't need to do anything.
    _db.rollback();
    _db.clearTimeout();

    // Reset, we can write now.
    _db.setQueryOnly(false);

    // Done.
    return returnValue;
}

BedrockCore::RESULT BedrockCore::processCommand(unique_ptr<BedrockCommand>& command, bool exclusive) {
    AutoTimer timer(command, exclusive ? BedrockCommand::BLOCKING_PROCESS : BedrockCommand::PROCESS);

    // Convenience references to commonly used properties.
    const SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;

    // Keep track of whether we've modified the database and need to perform a `commit`.
    bool needsCommit = false;
    try {
        SDEBUG("Processing '" << request.methodLine << "'");
        command->processCount++;
        _db.setTimeout(_getRemainingTime(command, true));
        if (!_db.insideTransaction()) {
            // If a transaction was already begun in `peek`, then this won't run. We call it here to support the case where
            // peek created a httpsRequest and closed it's first transaction until the httpsRequest was complete, in which
            // case we need to open a new transaction.
            if (!_db.beginTransaction(exclusive ? SQLite::TRANSACTION_TYPE::EXCLUSIVE : SQLite::TRANSACTION_TYPE::SHARED)) {
                STHROW("501 Failed to begin " + (exclusive ? "exclusive"s : "shared"s) + " transaction");
            }
            if (exclusive && command->writeConsistency != SQLiteNode::QUORUM) {
                decreaseCommandTimeout(command, BedrockCommand::DEFAULT_BLOCKING_TRANSACTION_COMMIT_LOCK_TIMEOUT);
            }
        }

        // If the command is mocked, turn on UpdateNoopMode.
        _db.setUpdateNoopMode(command->request.isSet("mockRequest"));

        // Process the command.
        {
            bool (*handler)(int, const char*, string&) = nullptr;
            bool enable = command->shouldEnableQueryRewriting(_db, &handler);
            AutoScopeRewrite rewrite(enable, _db, handler);
            try {
                command->reset(BedrockCommand::STAGE::PROCESS);
                command->process(_db);
                SDEBUG("Plugin '" << command->getName() << "' processed command '" << request.methodLine << "'");
            } catch (const SQLite::timeout_error& e) {
                if (!command->shouldSuppressTimeoutWarnings()) {
                    SALERT("Command " << command->request.methodLine << " timed out after " << e.time()/1000 << "ms.");
                }
                STHROW("555 Timeout processing command");
            }
        }

        // If we have no uncommitted query, just rollback the empty transaction. Otherwise, we need to commit.
        if (_db.getUncommittedQuery().empty() && !command->shouldCommitEmptyTransactions()) {
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
        _handleCommandException(command, e, &_db, &_server);
        _db.rollback();
        needsCommit = false;
    } catch (const SQLite::constraint_error& e) {
        SWARN("Unique Constraints Violation, command: " << request.methodLine);
        command->response.methodLine = "400 Unique Constraints Violation";
        _db.rollback();
        needsCommit = false;
    } catch(...) {
        SALERT("Unhandled exception typename: " << SGetCurrentExceptionName() << ", command: " << request.methodLine);
        command->response.methodLine = "500 Unhandled Exception";
        _db.rollback();
        needsCommit = false;
    }

    // We can turn this back off now, this is a noop if it's not turned on.
    _db.setUpdateNoopMode(false);

    // We can reset the timing info for the next command.
    _db.clearTimeout();

    // Done, return whether or not we need the parent to commit our transaction.
    command->complete = !needsCommit;

    return needsCommit ? RESULT::NEEDS_COMMIT : RESULT::NO_COMMIT_REQUIRED;
}

void BedrockCore::postProcessCommand(unique_ptr<BedrockCommand>& command, bool isBlockingCommitThread) {
    AutoTimer timer(command, isBlockingCommitThread ? BedrockCommand::BLOCKING_POSTPROCESS : BedrockCommand::POSTPROCESS);

    // Convenience references to commonly used properties.
    const SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;

    // We catch any exception and handle in `_handleCommandException`.
    try {
        try {
            SDEBUG("postProcessing at '" << request.methodLine << "' with priority: " << command->priority);
            command->postProcessCount++;
            _db.setTimeout(_getRemainingTime(command, false));

            // Make sure no writes happen while in postProcess command
            _db.setQueryOnly(true);

            // postProcess.
            command->postProcess(_db);
            SDEBUG("Plugin '" << command->getName() << "' postProcess command '" << request.methodLine << "'");

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
        } catch (const SQLite::timeout_error& e) {
            // Some plugins want to alert timeout errors themselves, and make them silent on bedrock.
            if (!command->shouldSuppressTimeoutWarnings()) {
                SALERT("Command " << command->request.methodLine << " timed out after " << e.time()/1000 << "ms.");
            }
            STHROW("555 Timeout postProcessing command");
        }
    } catch (const SException& e) {
        _handleCommandException(command, e, &_db, &_server);
    } catch (...) {
        SALERT("Unhandled exception typename: " << SGetCurrentExceptionName() << ", command: " << request.methodLine);
        command->response.methodLine = "500 Unhandled Exception";
    }

    // The command is complete.
    command->complete = true;
    _db.clearTimeout();

    // Reset, we can write now.
    _db.setQueryOnly(false);
}

void BedrockCore::_handleCommandException(unique_ptr<BedrockCommand>& command, const SException& e, SQLite* db, const BedrockServer* server) {
    string msg = "Error processing command '" + command->request.methodLine + "' (" + e.what() + "), ignoring.";

    STable logParams;
    if (!e.body.empty()) {
        logParams = SParseJSONObject(e.body);
    }

    if (SContains(e.what(), "_ALERT_")) {
        SALERT(msg, logParams);
    } else if (SContains(e.what(), "_WARN_")) {
        SWARN(msg, logParams);
    } else if (SContains(e.what(), "_HMMM_")) {
        SHMMM(msg, logParams);
    } else if (SStartsWith(e.what(), "50")) {
        SALERT(msg, logParams); // Alert on 500 level errors.
    } else {
        SINFO(msg, logParams);
    }

    // Set the response to the values from the exception, if set.
    if (!e.method.empty()) {
        command->response.methodLine = e.method;
    }
    if (!e.headers.empty()) {
        command->response.nameValueMap = e.headers;
    }
    if (!e.body.empty()) {
        command->response.content = e.body;
    }

    // Add the commitCount header to the response.
    if (db) {
        command->response["commitCount"] = to_string(db->getCommitCount());
    }

    if (server && server->args.isSet("-extraExceptionLogging")) {
        auto stack = e.details();
        command->response["exceptionSource"] = stack.back();
    }
}

void BedrockCore::decreaseCommandTimeout(unique_ptr<BedrockCommand>& command, uint64_t timeoutMS)
{
    const uint64_t remainingTimeUS = _getRemainingTime(command, false);
    if ((timeoutMS * 1000ull) < remainingTimeUS) {
        command->setTimeout(timeoutMS);
        const int64_t newRemainingTimeUS = _getRemainingTime(command, false);
        _db.setTimeout(newRemainingTimeUS);
        SINFO("Decreased command timeout from " << STIMESTAMP(STimeNow() + remainingTimeUS) << " to " << STIMESTAMP(STimeNow() + newRemainingTimeUS));
    }
}
