#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_DB : public BedrockPlugin {
  public:
    string getName() { return "DB"; }
    void initialize(const SData& args) { _args = args; }
    bool peekCommand(SQLite& db, BedrockCommand& command);
    bool processCommand(SQLite& db, BedrockCommand& command);

  private:
    // Attributes
    SData _args;
};
