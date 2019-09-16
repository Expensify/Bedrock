#include <libstuff/libstuff.h>
#include <BedrockPlugin.h>
#include <BedrockServer.h>

class TestHTTPSManager : public SHTTPSManager {
  public:
    TestHTTPSManager(const BedrockPlugin& plugin_) : SHTTPSManager(plugin_) { }
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
    virtual string getName() { return "TestPlugin"; }
    virtual bool preventAttach();
    bool peekCommand(SQLite& db, BedrockCommand& command);
    bool processCommand(SQLite& db, BedrockCommand& command);

  private:
    TestHTTPSManager* httpsManager;
    bool shouldPreventAttach = false;

    // This is a hack, but it lets one command store data for another command to read. This lets us inspect the output
    // of one command from another, even if the first command never sends a response.
    static mutex dataLock;
    static map<string, string> arbitraryData;
};
