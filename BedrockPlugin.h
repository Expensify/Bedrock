#pragma once
#include "BedrockCommand.h"
class BedrockServer;

// BREGISTER_PLUGIN is a macro to auto-instantiate a global instance of a plugin (_CT_). This lets plugin implementors
// just write `BREGISTER_PLUGIN(MyPluginName)` at the bottom of an implementation cpp file and get the plugin
// automatically registered with Bedrock.
// Why the weird `EXPAND{N}` macros? Because the pre-processor doesn't do recursive macro expansion with concatenation.
// See: http://stackoverflow.com/questions/1597007/
#define BREGISTER_PLUGIN_EXPAND2(_CT_, _NUM_) _CT_ __BREGISTER_PLUGIN_##_CT_##_NUM_
#define BREGISTER_PLUGIN_EXPAND1(_CT_, _NUM_) BREGISTER_PLUGIN_EXPAND2(_CT_, _NUM_)
#define BREGISTER_PLUGIN(_CT_) BREGISTER_PLUGIN_EXPAND1(_CT_, __COUNTER__)

// Simple plugin system to add functionality to a node at runtime.
class BedrockPlugin {
  public:
    // We use these sizes to make sure the storage engine does not silently truncate data. We throw an exception
    // instead.
    static constexpr int64_t MAX_SIZE_NONCOLUMN = 1024 * 1024 * 1024;
    static constexpr int64_t MAX_SIZE_QUERY = 1024 * 1024;
    static constexpr int64_t MAX_SIZE_BLOB = 1024 * 1024;
    static constexpr int64_t MAX_SIZE_SMALL = 255;

    // Utility functions for verifying expected input.
    static void verifyAttributeInt64(const SData& request, const string& name, size_t minSize);
    static void verifyAttributeSize(const SData& request, const string& name, size_t minSize, size_t maxSize);
    static void verifyAttributeBool(const SData& request, const string& name, bool require = true);

    // Standard constructor, inserts the created plugin in `g_registeredPluginList`.
    BedrockPlugin();

    // Returns a version string indicating the version of this plugin. This needs to be implemented in a thread-safe
    // manner, as it will be called from a different thread than any processing commands.
    virtual STable getInfo();

    // Returns the number of secure data entries required by this plugin.
    // If the plugin already has some data entries, this should return how many
    // remaining entries are required.
    virtual int needsSecureData();

    // Returns a short, descriptive name of this plugin
    virtual string getName();

    // Initializes it with command-line arguments and a reference to the server object that will call this plugin.
    // This may be called multiple times, it's up to a plugin to handle that in a reasonable way. Note that `server`
    // may change between calls to this function.
    virtual void initialize(const SData& args, BedrockServer& server);

    // Called to attempt to handle a command in a read-only fashion. Should return true if the command has been
    // completely handled and a response has been written into `command.response`, which can be returned to the client.
    // Should return `false` if the command needs to write to the database or otherwise could not be finished in a
    // read-only fashion (i.e., it opened an HTTPS request and is waiting for the response).
    virtual bool peekCommand(SQLite& db, BedrockCommand& command);

    // Called after a command has returned `false` to peek, and will attempt to commit and distribute a transaction
    // with any changes to the DB made by this plugin.
    virtual bool processCommand(SQLite& db, BedrockCommand& command);

    // Called at some point during initiation to allow the plugin to verify/change the database schema.
    virtual void upgradeDatabase(SQLite& db);

    // A list of SHTTPSManagers that the plugin would like the server to watch for activity. It is only guaranteed to
    // be safe to modify this list during `initialize`.
    list<SHTTPSManager*> httpsManagers;

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

    // Set to true if we don't want to log timeout alerts, and let the caller deal with it.
    virtual bool shouldSuppressTimeoutWarnings();

    // Loads a given secure data object into the plugins local secure data map.
    virtual void loadSecureData(SData& secureData);

  public:
    // A global static list of all registered plugins.
    static list<BedrockPlugin*>* g_registeredPluginList;

    // Look up a plugin by its name.
    static BedrockPlugin* getPluginByName(const string& name);
};
