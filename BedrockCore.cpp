#include "BedrockCore.h"
#include "BedrockPlugin.h"

BedrockCore::BedrockCore(SQLite& db) : 
SQLiteCore(db)
{ }

bool BedrockCore::peekCommand(SQLiteCommand& baseCommand)
{
    // Bedrock will always create these as BedrockCommands, and even when accepting commands from an SQLiteNode, it
    // converts them to the derived class before storing them.
    BedrockCommand& command = static_cast<BedrockCommand&>(baseCommand);

    // Classify the message
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;
    SDEBUG("Peeking at '" << request.methodLine << "'");

    // Assume success; will throw failure if necessary
    try {
        // Loop across the plugins to see which wants to take this
        bool pluginPeeked = false;
        for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
            // See if it peeks this
            if (plugin->enabled() && plugin->peekCommand(_db, command)) {
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
        _handleCommandException(command, e, false);
    } catch (const string e) {
        _handleCommandException(command, e, false);
    } catch (...) {
        _handleCommandException(command, "", false);
    }

    // If we get here, it means the command is fully completed.
    return true;
}

void BedrockCore::upgradeDatabase() 
{
    if (!_db.beginTransaction()) {
        throw "501 Failed to begin transaction";
    }

    for(BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
         if (plugin->enabled()) {
             plugin->upgradeDatabase(_db);
         }
    }
    SINFO("Finished upgrading database");
}

bool BedrockCore::processCommand(SQLiteCommand& baseCommand)
{
    // Bedrock will always create these as BedrockCommands, and even when accepting commands from an SQLiteNode, it
    // converts them to the derived class before storing them.
    BedrockCommand& command = static_cast<BedrockCommand&>(baseCommand);

    // Classify the message
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
            for(BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
                 // See if it processes this
                 if (plugin->enabled()) {
                     plugin->upgradeDatabase(_db);
                 }
             }
            SINFO("Finished upgrading database");
        } else {
            // All non-upgrade commands should be concurrent
            if (!_db.beginConcurrentTransaction()) {
                throw "501 Failed to begin concurrent transaction";
            }

            // Loop across the plugins to see which wants to take this
            bool pluginProcessed = false;
            for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
                // See if it processes this
                if (plugin->enabled() && plugin->processCommand(_db, command)) {
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

    // Done, return whether or not we need the parent to commit our transaction
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

#if 0
BedrockNode::BedrockNode(const SData& args, int threadID, int workerThreadCount, BedrockServer* server_)
    : SQLiteNode(args["-db"], args["-nodeName"], args["-nodeHost"], args.calc("-priority"), args.calc("-cacheSize"),
                 1024,                                                         // auto-checkpoint every 1024 pages
                 STIME_US_PER_M * 2 + SRandom::rand64() % STIME_US_PER_S * 30, // Be patient first time
                 server_->getVersion(), threadID, workerThreadCount, args.calc("-quorumCheckpoint"), args["-synchronousCommands"],
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

bool BedrockNode::_passToExternalQueue(Command* command) {
    // Check to see if we have a server
    // **FIXME: Given that this->server is defined in the constructor, how can this be false?
    if (server) {
        // No server, which probably means REASON
        server->enqueueCommand(command);
        return true;
    }
    return false;
};

void BedrockNode::postSelect(fd_map& fdm, uint64_t& nextActivity) {
    // Update the parent and attributes
    SQLiteNode::postSelect(fdm, nextActivity);
}

bool BedrockNode::isWorker() { return _worker; }

void BedrockNode::_setState(SQLCState state) {
    // When we change states, our schema might change - note that we need to
    // wait for any possible upgrade to complete again.
    _masterAndUpgradeComplete = false;
    SQLiteNode::_setState(state);
}

bool BedrockNode::dbReady() {
    return _masterAndUpgradeComplete;
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
#endif
