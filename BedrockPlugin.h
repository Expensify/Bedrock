#pragma once
#include "BedrockNode.h"

/* BREGISTER_PLUGIN is a macro to auto-instantiate a global instance of a plugin (_CT_). This lets plugin implementors
 * just write `BREGISTER_PLUGIN(MyPluginName)` at the bottom of an implementation cpp file and get the plugin
 * automatically registered with Bedrock.
 * Why the weird `EXPAND{N}` macros? Because the pre-processor doesn't do recursive macro expansion with concatenation.
 * See: http://stackoverflow.com/questions/1597007/
 */
#define BREGISTER_PLUGIN_EXPAND2(_CT_, _NUM_) _CT_ __BREGISTER_PLUGIN_##_CT_##_NUM_
#define BREGISTER_PLUGIN_EXPAND1(_CT_, _NUM_) BREGISTER_PLUGIN_EXPAND2(_CT_, _NUM_)
#define BREGISTER_PLUGIN(_CT_) BREGISTER_PLUGIN_EXPAND1(_CT_, __COUNTER__)

// Simple plugin system to add functionality to a node at runtime
class BedrockPlugin {
  public:
    /* We use these sizes to make sure the storage engine does not silently truncate data. We throw an exception
     * instead.
     */
    static constexpr int64_t MAX_SIZE_NONCOLUMN = 1024 * 1024 * 1024;
    static constexpr int64_t MAX_SIZE_QUERY = 1024 * 1024;
    static constexpr int64_t MAX_SIZE_BLOB = 1024 * 1024;
    static constexpr int64_t MAX_SIZE_SMALL = 255;

    static void verifyAttributeInt64(const SData& request, const string& name, size_t minSize);
    static void verifyAttributeSize(const SData& request, const string& name, size_t minSize, size_t maxSize);

    BedrockPlugin();

    // Returns a short, descriptive name of this plugin
    virtual string getName();

    // Initializes it with command-line arguments
    virtual void initialize(const SData& args);

    // Optionally updates the schema as necessary to support this plugin
    virtual void upgradeDatabase(BedrockNode* node, SQLite& db);

    // Optionally "peeks" at a command to process it in a read-only manner
    virtual bool peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);

    // Optionally "processes" a command in a read/write manner, triggering
    // a distributed transaction if necessary
    virtual bool processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);

    // Enable this plugin for active operation
    virtual void enable(bool enabled);
    virtual bool enabled();

    // Allow this node to do a self-test
    virtual void test(BedrockTester* tester);

    // The plugin can add as many of these as it needs in it's initialize function. The server will then add those
    // to it's own global list. Note that this is the *only* time these can be added, once `initialize` completes,
    // the server maintains a copy of this list. This also means that these managers need to live for the life of
    // the application.
    list<SHTTPSManager*> httpsManagers;

    // The plugin can register any number of timers it wants. When any of them `ding`, then the `timerFired`
    // function will be called, and passed the timer that is dinging.
    set<SStopwatch*> timers;
    virtual void timerFired(SStopwatch* timer);

  protected:
    // Attributes
    bool _enabled;

  public:
    // Global static attributes
    static list<BedrockPlugin*> g_registeredPluginList;
};
