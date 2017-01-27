// /srs/bedrock/BedrockNode.cpp
#include <libstuff/libstuff.h>
#include <libstuff/version.h>
#include "BedrockNode.h"
#include "BedrockPlugin.h"
#include "BedrockServer.h"
#include <iomanip>

// *jsonCode* Values
// -----------------------
// For consistency, all API commands return response codes in the following categories:
//
// ### 2xx Class ###
// Any response between 200 and 299 means the request was valid and accepted.
//
// * 200 OK
//
// ### 3xx Class ###
// Any response between 300 and 399 means that the request was valid, but rejected
// for some reason less than failure.
//
// * 300 Redundant request
// * 301 Limit hit.
// * 302 Invalid validateCode (for bank account validation)
//
// ### 4xx Class ###
// Any response between 400 and 499 means the request was valid, but failed.
//
// * 400 Unknown request failure
// * 401 Unauthorized
// * 402 Incomplete request
// * 403 Terrorist <-- no longer used, but left in for nostalgia.
// * 404 Resource doesn't exist
// * 405 Resource in incorrect state
// * 410 Resource not ready.
// * 411 Insufficient privileges
// * 412 Down for maintenance (used in waf)
//
// ### 5xx Class ###
// Any response between 500 and 599 indicates the server experienced some internal
// failure, and it's unknown if the request was valid.
//
// * 500 Unknown server failure
// * 501 Transaction failure
// * 502 Failed to execute query
// * 503 Query returned invalid response
// * 504 Resource in invalid state
// * 507 Vendor error
// * 508 Live operation not enabled
// * 509 Operation timed out.
// * 530 Unexpected response.
// * 531 Expected but unusable response, retry later.
// * 534 Unexpected HTTP request/response - usually timeout or 500 level server error.

BedrockNode::BedrockNode(const SData& args, int threadId, int threadCount, BedrockServer* server_)
    : SQLiteNode(args["-db"], args["-nodeName"], args["-nodeHost"], args.calc("-priority"), args.calc("-cacheSize"),
                 1024,                                                         // auto-checkpoint every 1024 pages
                 STIME_US_PER_M * 2 + SRandom::rand64() % STIME_US_PER_S * 30, // Be patient first time
                 server_->getVersion(), threadId, threadCount, args.calc("-quorumCheckpoint"), args["-synchronousCommands"],
                 args.test("-worker"), args.calc("-maxJournalSize")),
      server(server_) {
    // Initialize
    SINFO("BedrockNode constructor");
}

BedrockNode::~BedrockNode() {
    // Note any orphaned commands; this list should ideally be empty
    list<string> commandList;
    commandList = getQueuedCommandList();
    if (!commandList.empty())
        SALERT("Queued: " << SComposeJSONArray(commandList));
}

void BedrockNode::postSelect(fd_map& fdm, uint64_t& nextActivity) {
    // Update the parent and attributes
    SQLiteNode::postSelect(fdm, nextActivity);
}

bool BedrockNode::isWorker() { return _worker; }

bool BedrockNode::_peekCommand(SQLite& db, Command* command) {
    // Classify the message
    SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;
    SDEBUG("Peeking at '" << request.methodLine << "'");

    // Assume success; will throw failure if necessary
    try {
        // Loop across the plugins to see which wants to take this
        bool pluginPeeked = false;
        for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
            // See if it peeks this
            if (plugin->enabled() && plugin->peekCommand(this, db, command)) {
                // Peeked it!
                SINFO("Plugin '" << plugin->getName() << "' peeked command '" << request.methodLine << "'");
                pluginPeeked = true;
                break;
            }
        }

        // If not peeked by a plugin, do the old commands
        if (!pluginPeeked) {
            // Not a peekable command
            SINFO("Command '" << request.methodLine << "' is not peekable, queuing for processing.");
            return false; // Not done
        }

        // If no response was sent, assume 200 OK
        if (response.methodLine == "") {
            response.methodLine = "200 OK";
        }

        // Success.  If a command has set "content", encode it in the response.
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
        handleCommandException(db, command, e, false);
    } catch (const string e) {
        handleCommandException(db, command, e, false);
    } catch (...) {
        handleCommandException(db, command, "", false);
    }

    // If we get here, it means the command is fully completed.
    return true;
}

void BedrockNode::_setState(SQLCState state) {
    _dbReady = false;
    SQLiteNode::_setState(state);
}

void BedrockNode::setSyncNode(BedrockNode* node) {
    _syncNode = node;
}
bool BedrockNode::dbReady() {
    if (_syncNode) {
        return _syncNode->_dbReady;
    }

    return _dbReady;
}

void BedrockNode::_processCommand(SQLite& db, Command* command) {
    // Classify the message
    SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;
    SDEBUG("Received '" << request.methodLine << "'");
    try {
        if (SIEquals(request.methodLine, "UpgradeDatabase")) {
            if (!db.beginTransaction()) {
                throw "501 Failed to begin transaction";
            }
            // Loop across the plugins to give each an opportunity to upgrade the
            // database.  This command is triggered only on the MASTER, and only
            // upon it step up in the MASTERING state.
            SINFO("Upgrading database");
            for(BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
                 // See if it processes this
                 if (plugin->enabled()) {
                     plugin->upgradeDatabase(this, db);
                 }
             }
            SINFO("Finished upgrading database");
            _dbReady = true;
        } else {
            if (!db.beginConcurrentTransaction()) {
                throw "501 Failed to begin concurrent transaction";
            }
            // --------------------------------------------------------------------------
            // Loop across the plugins to see which wants to take this
            bool pluginProcessed = false;
            for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
                // See if it processes this
                if (plugin->enabled() && plugin->processCommand(this, db, command)) {
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
        bool isQueryEmpty = db.getUncommittedQuery().empty();
        if (isQueryEmpty)
            db.rollback();
        else if (!db.prepare())
            throw "501 Failed to prepare transaction";

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
        handleCommandException(db, command, e, true);
    } catch (const string e) {
        handleCommandException(db, command, e, true);
    } catch (...) {
        handleCommandException(db, command, "", true);
    }
}

void BedrockNode::handleCommandException(SQLite& db, Command* command, const string& e, bool wasProcessing) {
    // If we were peeking, then we weren't in a transaction. But if we were processing, we need to roll it back.
    if (wasProcessing) {
        db.rollback();
    }

    const string& msg = "Error processing command '" + command->request.methodLine + "' (" + e + "), ignoring: " +
                        command->request.serialize();

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
    if (command->response.methodLine == "") {
        command->response.methodLine = e;
    }

    // Re-throw, this is how worker threads know that processing failed.
    if (wasProcessing) {
        throw;
    }
}

// Notes that we failed to process something
void BedrockNode::_abortCommand(SQLite& db, Command* command) {
    // Note the failure in the response
    command->response.methodLine = "500 ABORTED";
}

void BedrockNode::_cleanCommand(Command* command) {
    if (command->httpsRequest) {
        command->httpsRequest->owner.closeTransaction(command->httpsRequest);
        command->httpsRequest = 0;
    }
}
