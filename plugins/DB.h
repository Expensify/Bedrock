#pragma once
#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_DB : public BedrockPlugin {
  public:
    BedrockPlugin_DB(BedrockServer& s);
    virtual const string& getName() const;
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);
    static const string name;
    virtual void upgradeDatabase(SQLite& db);
};

class BedrockDBCommand : public BedrockCommand {
  public:
    BedrockDBCommand(SQLiteCommand&& baseCommand, BedrockPlugin_DB* plugin);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);

  private:
    const string query;
};
