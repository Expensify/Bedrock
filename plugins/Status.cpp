#include "Status.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{" << node->name << ":" << getName() << "} "

const vector<string> BedrockPlugin_Status::statusCommandNames = {
    "GET /status/isSlave HTTP/1.1",
    "GET /status/handlingCommands HTTP/1.1",
    "Ping",
    "Status",
};

// ==========================================================================
bool BedrockPlugin_Status::peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) {
    // Pull out some helpful variables
    SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;

    // Admin Commands
    // --------------------
    //
    // ----------------------------------------------------------------------
    if (SIEquals(request.methodLine, statusCommandNames[0])) {
        // - GET /status/isSlave HTTP/1.1
        //
        //    Used for liveness check for HAProxy. Unfortunately it's limited to
        //    HTTP style requests for it's liveness checks, so let's pretend to be
        //    an HTTP server for this purpose. This allows us to load balance
        //    incoming requests.
        //
        //    **NOTE: HAProxy interprets 2xx/3xx level responses as alive
        //                               4xx/5xx level responses as dead
        //
        //    Also we must prepend response status with HTTP/1.1
        if (node->getState() == SQLC_SLAVING)
            response.methodLine = "HTTP/1.1 200 Slaving";
        else
            response.methodLine = "HTTP/1.1 500 Not slaving. State=" + (string)SQLCStateNames[node->getState()];
        return true; // Successfully peeked
    }

    // --------------------------------------------------------------------------
    else if (SIEquals(request.methodLine, statusCommandNames[1])) {
        // - GET /status/handlingCommands HTTP/1.1
        //
        //    Used for liveness check for HAProxy. Unfortunately it's limited to
        //    HTTP style requests for it's liveness checks, so let's pretend to be
        //    an HTTP server for this purpose. This allows us to load balance
        //    between the www nodes and the db nodes. This command returns 200
        //    if this node should be given new commands
        //
        //     **NOTE: HAProxy interprets 2xx/3xx level responses as alive
        //                                4xx/5xx level responses as dead
        //
        //             Also we must prepend response status with HTTP/1.1
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
    else if (SIEquals(request.methodLine, statusCommandNames[2])) {
        // - Ping( )
        //
        //     For liveness tests.
        //
        return true; // Successfully peeked
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, statusCommandNames[3])) {
        // - Status( )
        //
        //     Give us some data on this server.
        // TODO Some or all of this data might be wrong as worker threads can now respond this, but previously didn't
        // have access to all of this information.
        list<string> plugins;
        for (BedrockPlugin* plugin : *g_registeredPluginList) {
            STable pluginData;
            pluginData["name"] = plugin->getName();
            STable pluginInfo = plugin->getInfo();
            for (pair<string, string> row : pluginInfo) {
                pluginData[row.first] = row.second;
            }
            plugins.push_back(SComposeJSONObject(pluginData));
        }
        content["plugins"] = SComposeJSONArray(plugins);

        content["state"] = SQLCStateNames[node->getState()];
        content["hash"] = node->getHash();
        content["commitCount"] = SToStr(node->getCommitCount());
        content["version"] = _args ? (*_args)["version"] : "";
        content["priority"] = SToStr(node->getPriority());
        list<string> peerList;
        for (BedrockNode::Peer* peer : node->peerList) {
            STable peerTable = peer->nameValueMap;
            peerTable["host"] = peer->host;
            peerList.push_back(SComposeJSONObject(peerTable));
        }
        content["peerList"] = SComposeJSONArray(peerList);
        content["queuedCommandList"] = SComposeJSONArray(node->getQueuedCommandList());
        content["escalatedCommandList"] = SComposeJSONArray(node->getEscalatedCommandList());
        content["processedCommandList"] = SComposeJSONArray(node->getProcessedCommandList());
        content["isMaster"] = node->getState() == SQLC_MASTERING ? "true" : "false";
        return true; // Successfully peeked
    }

    // Didn't recognize this command
    return false;
}

// Because we don't have 'node' here.
#undef SLOGPREFIX
#define SLOGPREFIX "{:" << getName() << "} "
void BedrockPlugin_Status::initialize(const SData& args) {
    _args = &args;
}
