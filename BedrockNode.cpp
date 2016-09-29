/// p /srs/bedrock/BedrockNode.cpp
#include <libstuff/libstuff.h>
#include <libstuff/version.h>
#include "BedrockNode.h"
#include "BedrockServer.h"
#include <iomanip>

// Global static values
list<BedrockNode::Plugin*>* BedrockNode::Plugin::g_registeredPluginList = 0;

/// p *jsonCode* Values
/// p -----------------------
/// p For consistency, all API commands return response codes in the following categories:
/// p
/// p ### 2xx Class ###
/// p Any response between 200 and 299 means the request was valid and accepted.
/// p
/// p * 200 OK
/// p
/// p ### 3xx Class ###
/// p Any response between 300 and 399 means that the request was valid, but rejected
/// p for some reason less than failure.
/// p
/// p * 300 Redundant request
/// p * 301 Limit hit.
/// p * 302 Invalid validateCode (for bank account validation)
/// p
/// p ### 4xx Class ###
/// p Any response between 400 and 499 means the request was valid, but failed.
/// p
/// p * 400 Unknown request failure
/// p * 401 Unauthorized
/// p * 402 Incomplete request
/// p * 403 Terrorist <-- no longer used, but left in for nostalgia.
/// p * 404 Resource doesn't exist
/// p * 405 Resource in incorrect state
/// p * 410 Resource not ready.
/// p * 411 Insufficient privileges
/// p * 412 Down for maintenance (used in waf)
/// p
/// p ### 5xx Class ###
/// p Any response between 500 and 599 indicates the server experienced some internal
/// p failure, and it's unknown if the request was valid.
/// p
/// p * 500 Unknown server failure
/// p * 501 Transaction failure
/// p * 502 Failed to execute query
/// p * 503 Query returned invalid response
/// p * 504 Resource in invalid state
/// p * 507 Vendor error
/// p * 508 Live operation not enabled
/// p * 509 Operation timed out.
/// p * 530 Unexpected response.
/// p * 531 Expected but unusable response, retry later.
/// p * 534 Unexpected HTTP request/response - usually timeout or 500 level server error.
/// p

// --------------------------------------------------------------------------
BedrockNode::BedrockNode(const SData& args, BedrockServer* server_)
    : SQLiteNode(args["-db"], args["-nodeName"], args["-nodeHost"], args.calc("-priority"), args.calc("-cacheSize"),
                 1024,                                                 // auto-checkpoint every 1024 pages
                 STIME_US_PER_M * 2 + SRand64() % STIME_US_PER_S * 30, // Be patient first time
                 server_->getVersion(), args.calc("-quorumCheckpoint"), args["-synchronousCommands"],
                 args.test("-readOnly"), args.calc("-maxJournalSize")),
      server(server_)
{
    // Initialize
    SINFO("BedrockNode constructor");
}

// --------------------------------------------------------------------------
BedrockNode::~BedrockNode()
{
    // Note any orphaned commands; this list should ideally be empty
    list<string> commandList;
    commandList = getQueuedCommandList();
    if (!commandList.empty())
        SALERT("Queued: " << SComposeJSONArray(commandList));
}

// --------------------------------------------------------------------------
void BedrockNode::postSelect(fd_map& fdm, uint64_t& nextActivity)
{
    // Update the parent and attributes
    SQLiteNode::postSelect(fdm, nextActivity);
}

bool BedrockNode::isReadOnly() { return _readOnly; }

/// Read-Only Command Definitions
/// ------------------------------
bool BedrockNode::_peekCommand(SQLite& db, Command* command)
{
    // Classify the message
    SData& request  = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;
    SDEBUG("Peeking at '" << request.methodLine << "'");

    // Assume success; will throw failure if necessary
    response.methodLine = "200 OK";
    try {
        // Loop across the plugins to see which wants to take this
        bool pluginPeeked = false;
        SFOREACH (list<Plugin*>, *Plugin::g_registeredPluginList, pluginIt) {
            // See if it peeks this
            Plugin* plugin = *pluginIt;
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
        // Error -- roll back the database and return the error
        const string& msg = "Error processing read-only command '" + request.methodLine + "' (" + e + "), ignoring: " +
                            request.serialize();
        if (SContains(e, "_ALERT_"))
            SALERT(msg);
        else if (SContains(e, "_WARN_"))
            SWARN(msg);
        else if (SContains(e, "_HMMM_"))
            SHMMM(msg);
        else if (SStartsWith(e, "50"))
            SALERT(msg); // Alert on 500 level errors.
        else
            SINFO(msg);
        response.methodLine = e;
    }

    // If we get here, it means the command is fully completed.
    return true;
}

// --------------------------------------------------------------------------
///
/// Read-Write Command Definitions
/// -------------------------------
void BedrockNode::_processCommand(SQLite& db, Command* command)
{
    // Classify the message
    SData& request  = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;
    SDEBUG("Received '" << request.methodLine << "'");
    try {
        // Process the message
        if (!db.beginTransaction())
            throw "501 Failed to begin transaction";

        // --------------------------------------------------------------------------
        if (SIEquals(request.methodLine, "UpgradeDatabase")) {
            // Loop across the plugins to give each an opportunity to upgrade the
            // database.  This command is triggered only on the MASTER, and only
            // upon it step up in the MASTERING state.
            SINFO("Upgrading database");
            for_each(Plugin::g_registeredPluginList->begin(), Plugin::g_registeredPluginList->end(),
                     [this, &db](Plugin* plugin) {
                         // See if it processes this
                         if (plugin->enabled()) {
                             plugin->upgradeDatabase(this, db);
                         }
                     });
            SINFO("Finished upgrading database");
        } else {
            // --------------------------------------------------------------------------
            // Loop across the plugins to see which wants to take this
            bool pluginProcessed = false;
            SFOREACH (list<Plugin*>, *Plugin::g_registeredPluginList, pluginIt) {
                // See if it processes this
                Plugin* plugin = *pluginIt;
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
        // Error -- roll back the database and return the error
        db.rollback();
        const string& msg =
            "Error processing command '" + request.methodLine + "' (" + e + "), ignoring: " + request.serialize();
        if (SContains(e, "_ALERT_"))
            SALERT(msg);
        else if (SContains(e, "_WARN_"))
            SWARN(msg);
        else if (SContains(e, "_HMMM_"))
            SHMMM(msg);
        else if (SStartsWith(e, "50"))
            SALERT(msg); // Alert on 500 level errors.
        else
            SINFO(msg);
        response.methodLine = e;
    }
}

// --------------------------------------------------------------------------
// Notes that we failed to process something
void BedrockNode::_abortCommand(SQLite& db, Command* command)
{
    // Note the failure in the response
    command->response.methodLine = "500 ABORTED";
}

// --------------------------------------------------------------------------
void BedrockNode::_cleanCommand(Command* command)
{
    if (command->httpsRequest) {
        if (command->httpsRequest->owner) {
            command->httpsRequest->owner->closeTransaction(command->httpsRequest);
        } else {
            SERROR("No owner for this https request " << command->httpsRequest->fullResponse.methodLine);
        }
        command->httpsRequest = 0;
    }
}

// --------------------------------------------------------------------------
BedrockNode::Plugin::Plugin()
{
    // Auto-register this instance into the global static list, initializing
    // the list if that hasn't yet been done. This just makes it available for
    // enabling via the command line: by default all plugins start out
    // disabled.
    //
    // **NOTE: This code runs *before* main().  This means that libstuff
    //         hasn't yet been initialized, so there is no logging.
    if (!g_registeredPluginList) {
        g_registeredPluginList = new list<BedrockNode::Plugin*>;
    }
    _enabled = false;
    g_registeredPluginList->push_back(this);
}
