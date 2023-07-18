#pragma once
#include <libstuff/libstuff.h>
#include <BedrockPlugin.h>
#include <BedrockServer.h>

class TestHTTPSManager : public SHTTPSManager {
  public:
    TestHTTPSManager(BedrockPlugin& plugin_) : SHTTPSManager(plugin_) { }
    Transaction* send(const string& url, const SData& request);
    virtual bool _onRecv(Transaction* transaction);

    // Like _httpsSend in the base class, but doesn't actually send, so we can test timeouts.
    Transaction* httpsDontSend(const string& url, const SData& request);
    virtual ~TestHTTPSManager();
};

class BedrockPlugin_TestPlugin : public BedrockPlugin
{
  public:
    BedrockPlugin_TestPlugin(BedrockServer& s);
    ~BedrockPlugin_TestPlugin();
    void upgradeDatabase(SQLite& db);
    virtual const string& getName() const;
    static const string name;
    virtual bool preventAttach();
    void stateChanged(SQLite& db, SQLiteNodeState newState);

    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);

    TestHTTPSManager* httpsManager;
    bool shouldPreventAttach = false;

    // This is a hack, but it lets one command store data for another command to read. This lets us inspect the output
    // of one command from another, even if the first command never sends a response.
    static mutex dataLock;
    static map<string, string> arbitraryData;

    // Tests setting of an ID value in stateChanged after an upgrade is completed.
    atomic<int64_t> _maxID;
};

class TestPluginCommand : public BedrockCommand {
  public:
    TestPluginCommand(SQLiteCommand&& baseCommand, BedrockPlugin_TestPlugin* plugin);
    ~TestPluginCommand();
    virtual void prePeek(SQLite& db);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);
    virtual void postProcess(SQLite& db);
    virtual bool shouldPrePeek();
    virtual bool shouldPostProcess();
    virtual void reset(BedrockCommand::STAGE stage) override;

  private:
    BedrockPlugin_TestPlugin& plugin() { return static_cast<BedrockPlugin_TestPlugin&>(*_plugin); }

    bool pendingResult;
    string chainedHTTPResponseContent;
    string urls;
};
