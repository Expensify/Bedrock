#include <libstuff/libstuff.h>
#include <BedrockPlugin.h>

class TestHTTPSMananager : public SHTTPSManager {
  public:
    Transaction* send(const string& url, const SData& request);

    virtual bool _onRecv(Transaction* transaction);
    virtual ~TestHTTPSMananager();
};

class BedrockPlugin_TestPlugin : public BedrockPlugin
{
  public:
    virtual string getName() { return "TestPlugin"; }
    void initialize(const SData& args);

    bool peekCommand(SQLite& db, BedrockCommand& command);
    bool processCommand(SQLite& db, BedrockCommand& command);

  private:
    TestHTTPSMananager httpsManager;
};
