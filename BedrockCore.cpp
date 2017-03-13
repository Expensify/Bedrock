#include "BedrockCore.h"
#include "BedrockPlugin.h"
#include "BedrockServer.h"

BedrockCore::BedrockCore(SQLite& db, const BedrockServer& server) : 
SQLiteCore(db),
_server(server)
{ }

bool BedrockCore::peekCommand(BedrockCommand& command)
{
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;
    SDEBUG("Peeking at '" << request.methodLine << "'");

    // We catch any exception and handle in `_handleCommandException`.
    try {
        // Try each plugin, and go with the first one that says it succeeded.
        bool pluginPeeked = false;
        for (auto plugin : _server.plugins) {
            // See if it peeks this
            if (plugin->peekCommand(_db, command)) {
                // Peeked it!
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

        // If no response was sent, assume 200 OK
        if (response.methodLine == "") {
            response.methodLine = "200 OK";
        }

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

void BedrockCore::upgradeDatabase() 
{
    if (!_db.beginTransaction()) {
        throw "501 Failed to begin transaction";
    }
    for (auto plugin : _server.plugins) {
        plugin->upgradeDatabase(_db);
    }
    SINFO("Finished upgrading database");
}

bool BedrockCore::processCommand(BedrockCommand& command)
{
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;
    SDEBUG("Received '" << request.methodLine << "'");
    bool needsCommit = false;
    try {
        if (SIEquals(request.methodLine, "UpgradeDatabase")) {
            // Begin a non-concurrent transaction for database upgrading
            if (!_db.beginTransaction()) {
                throw "501 Failed to begin transaction";
            }

            // Loop across the plugins to give each an opportunity to upgrade the
            // database.  This command is triggered only on the MASTER, and only
            // upon it step up in the MASTERING state.
            SINFO("Upgrading database");
            for(auto plugin : _server.plugins) {
                plugin->upgradeDatabase(_db);
            }
            SINFO("Finished upgrading database");
        } else {
            // All non-upgrade commands should be concurrent
            if (!_db.beginConcurrentTransaction()) {
                throw "501 Failed to begin concurrent transaction";
            }

            // Loop across the plugins to see which wants to take this
            bool pluginProcessed = false;
            for (auto plugin : _server.plugins) {
                // See if it processes this
                if (plugin->processCommand(_db, command)) {
                    // Processed it!
                    SINFO("Plugin '" << plugin->getName() << "' processed command '" << request.methodLine << "'");
                    pluginProcessed = true;
                    break;
                }
            }

            // If no plugin processed it, respond accordingly
            if (!pluginProcessed) {
                // No command specified
                SWARN("Command '" << request.methodLine << "' does not exist.");
                throw "430 Unrecognized command";
            }
        }

        // If we have no uncommitted query, just rollback the empty transaction.
        // Otherwise, try to prepare to commit.
        bool isQueryEmpty = _db.getUncommittedQuery().empty();
        if (isQueryEmpty) {
            _db.rollback();
        } else {
            needsCommit = true;
        }

        // If no response was sent, assume 200 OK
        if (response.methodLine == "") {
            response.methodLine = "200 OK";
        }

        // Success, this command will be committed.
        SINFO("Responding '" << response.methodLine << "' to '" << request.methodLine << "'.");

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
    if (command.response.methodLine == "") {
        command.response.methodLine = e;
    }
}

