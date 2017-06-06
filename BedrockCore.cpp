#include "BedrockCore.h"
#include "BedrockPlugin.h"
#include "BedrockServer.h"

BedrockCore::BedrockCore(SQLite& db, const BedrockServer& server) : 
SQLiteCore(db),
_server(server)
{ }

bool BedrockCore::peekCommand(BedrockCommand& command) {
    AutoTimer timer(command, BedrockCommand::PEEK);
    // Convenience references to commonly used properties.
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;
    SDEBUG("Peeking at '" << request.methodLine << "'");
    command.peekCount++;

    // We catch any exception and handle in `_handleCommandException`.
    try {
        // Try each plugin, and go with the first one that says it succeeded.
        bool pluginPeeked = false;
        for (auto plugin : _server.plugins) {
            // Try to peek the command.
            if (plugin->peekCommand(_db, command)) {
                SINFO("Plugin '" << plugin->getName() << "' peeked command '" << request.methodLine << "'");
                pluginPeeked = true;
                break;
            }
        }

        // If nobody succeeded in peeking it, then we'll need to process it.
        // TODO: Would be nice to be able to check if a plugin *can* handle a command, so that we can differentiate
        // between "didn't peek" and "peeked but didn't complete".
        if (!pluginPeeked) {
            SINFO("Command '" << request.methodLine << "' is not peekable, queuing for processing.");
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
    } catch (const char* e) {
        _handleCommandException(command, e, false);
    } catch (const string e) {
        _handleCommandException(command, e, false);
    } catch (...) {
        _handleCommandException(command, "", false);
    }

    // If we get here, it means the command is fully completed.
    command.complete = true;
    return true;
}

bool BedrockCore::processCommand(BedrockCommand& command) {
    AutoTimer timer(command, BedrockCommand::PROCESS);
    // Convenience references to commonly used properties.
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;
    SDEBUG("Processing '" << request.methodLine << "'");
    command.processCount++;

    // Keep track of whether we've modified the database and need to perform a `commit`.
    bool needsCommit = false;
    try {
        if (!_db.beginConcurrentTransaction()) {
            throw "501 Failed to begin concurrent transaction";
        }

        // Loop across the plugins to see which wants to take this.
        bool pluginProcessed = false;
        for (auto plugin : _server.plugins) {
            // Try to process the command.
            if (plugin->processCommand(_db, command)) {
                SINFO("Plugin '" << plugin->getName() << "' processed command '" << request.methodLine << "'");
                pluginProcessed = true;
                break;
            }
        }

        // If no plugin processed it, respond accordingly.
        if (!pluginProcessed) {
            SWARN("Command '" << request.methodLine << "' does not exist.");
            throw "430 Unrecognized command";
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

        // Add the commitCount header to the response.
        response["commitCount"] = to_string(_db.getCommitCount());

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
    } catch (const char* e) {
        _handleCommandException(command, e, true);
    } catch (const string e) {
        _handleCommandException(command, e, true);
    } catch (...) {
        _handleCommandException(command, "", true);
    }

    // Done, return whether or not we need the parent to commit our transaction.
    command.complete = !needsCommit;
    return needsCommit;
}

void BedrockCore::_handleCommandException(BedrockCommand& command, const string& e, bool wasProcessing) {
    // If we were peeking, then we weren't in a transaction. But if we were processing, we need to roll it back.
    if (wasProcessing) {
        _db.rollback();
    }
    const string& msg = "Error processing command '" + command.request.methodLine + "' (" + e + "), ignoring: " +
                        command.request.serialize();
    if (SContains(e, "_ALERT_")) {
        SALERT(msg);
    } else if (SContains(e, "_WARN_")) {
        SWARN(msg);
    } else if (SContains(e, "_HMMM_")) {
        SHMMM(msg);
    } else if (SStartsWith(e, "50")) {
        SALERT(msg); // Alert on 500 level errors.
    } else {
        SINFO(msg);
    }

    // If the command set a response before throwing an exception, we'll keep that as our response to use. Otherwise,
    // we'll use the text of the error.
    if (command.response.methodLine.empty()) {
        command.response.methodLine = e;
    }

    // Add the commitCount header to the response.
    command.response["commitCount"] = to_string(_db.getCommitCount());
}
