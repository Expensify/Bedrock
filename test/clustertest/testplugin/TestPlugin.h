#include <libstuff/libstuff.h>
#include <BedrockPlugin.h>
#include <BedrockServer.h>

class TestHTTPSMananager : public SHTTPSManager {
  public:
    Transaction* send(const string& url, const SData& request);
    virtual bool _onRecv(Transaction* transaction);

    // Like _httpsSend in the base class, but doesn't actually send, so we can test timeouts.
    Transaction* httpsDontSend(const string& url, const SData& request);
    virtual ~TestHTTPSMananager();
};

class BedrockPlugin_TestPlugin : public BedrockPlugin
{
  public:
    BedrockPlugin_TestPlugin();
    void upgradeDatabase(SQLite& db);
    virtual string getName() { return "TestPlugin"; }
    virtual bool preventAttach();
    void initialize(const SData& args, BedrockServer& server);
    bool peekCommand(SQLite& db, BedrockCommand& command);
    bool processCommand(SQLite& db, BedrockCommand& command);

  private:
    TestHTTPSMananager httpsManager;
    BedrockServer* _server;
    bool shouldPreventAttach = false;
};
