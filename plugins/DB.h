#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below

class BedrockDBCommand : public BedrockCommand {
  public:
    BedrockDBCommand(SQLiteCommand&& baseCommand);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);
    virtual const string& getName();
  private:
    static const string name;
};

class BedrockPlugin_DB : public BedrockPlugin {
  public:
    BedrockPlugin_DB(BedrockServer& s);
    virtual string getName() { return "DB"; }
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);
};
