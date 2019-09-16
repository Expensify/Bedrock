#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_DB : public BedrockPlugin {
  public:
    BedrockPlugin_DB(BedrockServer& s);
    virtual string getName() { return "DB"; }
    virtual bool peekCommand(SQLite& db, BedrockCommand& command);
    virtual bool processCommand(SQLite& db, BedrockCommand& command);
};
