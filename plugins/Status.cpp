#include "Status.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{" << node->name << ":" << getName() << "} "

// ==========================================================================
bool BedrockPlugin_Status::peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) {
    // Pull out some helpful variables
    SData& request = command->request;
    SData& response = command->response;
    STable& content = command->jsonContent;

    //
    // Admin Commands
    // --------------------
    //
    // ----------------------------------------------------------------------
    if (SIEquals(request.methodLine, "GET /status/isSlave HTTP/1.1")) {
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
    else if (SIEquals(request.methodLine, "GET /status/handlingCommands HTTP/1.1")) {
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
    else if (SIEquals(request.methodLine, "Ping")) {
        // - Ping( )
        //
        //     For liveness tests.
        //
        return true; // Successfully peeked
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "Status")) {
        // - Status( )
        //
        //     Give us some data on this server.
        list<string> plugins;
        for_each(g_registeredPluginList->begin(), g_registeredPluginList->end(), [&](BedrockPlugin* plugin){
            STable pluginData;
            pluginData["name"] = plugin->getName();
            STable pluginInfo = plugin->getInfo();
            for_each(pluginInfo.begin(), pluginInfo.end(), [&](pair<string, string> row){
                pluginData[row.first] = row.second;
            });
            plugins.push_back(SComposeJSONObject(pluginData));
        });
        content["plugins"] = SComposeJSONArray(plugins);

        content["state"] = SQLCStateNames[node->getState()];
        content["hash"] = node->getHash();
        content["commitCount"] = SToStr(node->getCommitCount());
        content["version"] = _args ? (*_args)["version"] : "";
        content["priority"] = SToStr(node->getPriority());
        list<string> peerList;
        SFOREACH (list<BedrockNode::Peer*>, node->peerList, it) {
            STable peer = (*it)->nameValueMap;
            peer["host"] = (*it)->host;
            peerList.push_back(SComposeJSONObject(peer));
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
