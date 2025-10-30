#pragma once
#include "BedrockCommand.h"
class BedrockServer;

// Simple plugin system to add functionality to a node at runtime.
class BedrockPlugin {
  public:
    // We use these sizes to make sure the storage engine does not silently truncate data. We throw an exception
    // instead.
    static constexpr int64_t MAX_SIZE_QUERY = 1024 * 1024;
    static constexpr int64_t MAX_SIZE_BLOB = 1024 * 1024;
    static constexpr int64_t MAX_SIZE_SMALL = 255;

    /**
     * Check if a date or datetime is valid
     */
    static bool isValidDate(const string& date);

    // Utility functions for verifying expected input.
    static void verifyAttributeInt64(const SData& request, const string& name, size_t minSize);
    static void verifyAttributeSize(const SData& request, const string& name, size_t minSize, size_t maxSize);
    static void verifyAttributeBool(const SData& request, const string& name, bool require = true);
    static void verifyAttributeDate(const SData& request, const char* key, bool require);

    BedrockPlugin(BedrockServer& s);
    virtual ~BedrockPlugin();

    // Returns a version string indicating the version of this plugin. This needs to be implemented in a thread-safe
    // manner, as it will be called from a different thread than any processing commands.
    virtual STable getInfo();

    // Returns a short, descriptive name of this plugin
    virtual const string& getName() const;

    // Return a command, or a null pointer if this plugin can't handle this request.
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand) = 0;

    // Called at some point during initiation to allow the plugin to verify/change the database schema.
    virtual void upgradeDatabase(SQLite& db);

    // The plugin can register any number of timers it wants. When any of them `ding`, then the `timerFired`
    // function will be called, and passed the timer that is dinging.
    set<SStopwatch*> timers;
    virtual void timerFired(SStopwatch* timer);

    // Below here are several functions for allowing plugins to open a port and accept their own connections.
    // Returns "host:port" on which to listen, or empty if none
    virtual string getPort() { return ""; }

    // Called when a socket is accepted on this plugin's port
    virtual void onPortAccept(STCPManager::Socket* s) {}

    // Called when a socket receives input
    // request: optional request to queue internally
    virtual void onPortRecv(STCPManager::Socket* s, SData& request) { }

    // After processing the request from this plugin, this is called to send the response
    // response The response from the processed request
    // s        Optional socket from which this request was received
    virtual void onPortRequestComplete(const BedrockCommand& command, STCPManager::Socket* s) { }

    virtual bool preventAttach();

    // Called when a client or plugin requests that the BedrockServer detaches from the database.
    // If a plugin makes it's own connections to the database, it should use this function to
    // close those connections. A plugin that implements this should expect it will be called
    // multiple times. The subsequent calls should be a no-op.
    virtual void onDetach() {}

    // Called when a client or plugin requests that the BedrockServer re-attache to the database.
    // If a plugin makes it's own connections to the database, it should use this function to
    // re-open those connections if not handled elsewhere in the plugin.
    virtual void onAttach() {}

    // Called when a node changes state
    virtual void stateChanged(SQLite& db, SQLiteNodeState newState) {}

    // Called when the sync thread is finishing, before destroying DB handles.
    virtual void serverStopping() {}

    // Should a conflict on the given tableName result in locking the associated database page when we try to commit again?
    virtual bool shouldLockCommitPageOnTableConflict(const string& tableName) const;

    // Map of plugin names to functions that will return a new plugin of the given type.
    static map<string, function<BedrockPlugin*(BedrockServer&)>> g_registeredPluginList;

    // Reference to the BedrockServer object that owns this plugin.
    BedrockServer& server;
};
