////p /src/bedrock/plugins/Status.cpp
#include <libstuff/libstuff.h>
#include <libstuff/version.h>
#include "../BedrockPlugin.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{" << node->name << ":" << getName() << "} "

// Declare the class we're going to implement below
class BedrockPlugin_Status : public BedrockNode::Plugin
{
  public:
    virtual string getName()
    {
        return "Status";
    }
    virtual bool peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
};

// Register for auto-discovery at boot
BREGISTER_PLUGIN(BedrockPlugin_Status);

// ==========================================================================
bool BedrockPlugin_Status::peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command)
{
    // Pull out some helpful variables
    SData& request  = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;

    /// p
    /// p Admin Commands
    /// p --------------------
    /// p
    // ----------------------------------------------------------------------
    if (SIEquals(request.methodLine, "GET /status/isSlave HTTP/1.1")) {
        /// p - GET /status/isSlave HTTP/1.1
        /// p
        /// p    Used for liveness check for HAProxy. Unfortunately it's limited to
        /// p    HTTP style requests for it's liveness checks, so let's pretend to be
        /// p    an HTTP server for this purpose. This allows us to load balance
        /// p    incoming requests.
        /// p
        /// p    **NOTE: HAProxy interprets 2xx/3xx level responses as alive
        /// p                               4xx/5xx level responses as dead
        /// p
        /// p    Also we must prepend response status with HTTP/1.1
        if (node->getState() == SQLC_SLAVING)
            response.methodLine = "HTTP/1.1 200 Slaving";
        else
            response.methodLine = "HTTP/1.1 500 Not slaving. State=" + (string)SQLCStateNames[node->getState()];
        return true; // Successfully peeked
    }

    // --------------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "GET /status/handlingCommands HTTP/1.1")) {
        /// p - GET /status/handlingCommands HTTP/1.1
        /// p
        /// p    Used for liveness check for HAProxy. Unfortunately it's limited to
        /// p    HTTP style requests for it's liveness checks, so let's pretend to be
        /// p    an HTTP server for this purpose. This allows us to load balance
        /// p    between the www nodes and the db nodes. This command returns 200
        /// p    if this node should be given new commands
        /// p
        /// p     **NOTE: HAProxy interprets 2xx/3xx level responses as alive
        /// p                                4xx/5xx level responses as dead
        /// p
        /// p             Also we must prepend response status with HTTP/1.1
        if (node->getState() != SQLC_SLAVING) {
            response.methodLine = "HTTP/1.1 500 Not slaving. State=" + (string)SQLCStateNames[node->getState()];
        } else if (node->getVersion() != node->getMasterVersion()) {
            response.methodLine = "HTTP/1.1 500 Mismatched version. Version=" + node->getVersion();
        } else {
            response.methodLine = "HTTP/1.1 200 Slaving";
        }
        return true; // Successfully peeked
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "Ping")) {
        /// p - Ping( )
        /// p
        /// p     For liveness tests.
        /// p
        return true; // Successfully peeked
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "Status")) {
        /// p - Status( )
        /// p
        /// p     Give us some data on this server.
        /// p
        content["state"]       = SQLCStateNames[node->getState()];
        content["hash"]        = node->getHash();
        content["commitCount"] = SToStr(node->getCommitCount());
        content["version"]     = SVERSION;
        content["priority"]    = SToStr(node->getPriority());
        list<string> peerList;
        SFOREACH (list<BedrockNode::Peer*>, node->peerList, it) {
            STable peer  = (*it)->nameValueMap;
            peer["host"] = (*it)->host;
            peerList.push_back(SComposeJSONObject(peer));
        }
        content["peerList"]             = SComposeJSONArray(peerList);
        content["queuedCommandList"]    = SComposeJSONArray(node->getQueuedCommandList());
        content["escalatedCommandList"] = SComposeJSONArray(node->getEscalatedCommandList());
        content["processedCommandList"] = SComposeJSONArray(node->getProcessedCommandList());
        content["isMaster"]             = node->getState() == SQLC_MASTERING ? "true" : "false";
        return true; // Successfully peeked
    }

    // Didn't recognize this command
    return false;
}
