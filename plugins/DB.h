#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_DB : public BedrockPlugin {
  public:
    virtual string getName() { return "DB"; }
    virtual void initialize(const SData& args, BedrockServer& server) { _args = args; }
    virtual bool peekCommand(SQLite& db, BedrockCommand& command);
    virtual bool processCommand(SQLite& db, BedrockCommand& command);

    static bool rewriteHandler(int actionCode, const char* table, string& newQuery);
    bool shouldEnableQueryRewriting(const SQLite& db, const BedrockCommand& command, bool (**handler)(int, const char*, string&));

  private:
    // Attributes
    SData _args;
};
