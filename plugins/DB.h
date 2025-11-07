#pragma once
#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_DB : public BedrockPlugin {
  public:
    BedrockPlugin_DB(BedrockServer& s);
    virtual const string& getName() const;
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);
    static const string name;
};

class BedrockDBCommand : public BedrockCommand {
  public:
    BedrockDBCommand(SQLiteCommand&& baseCommand, BedrockPlugin_DB* plugin);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);

  private:
    string query;

    // Callback for SQLite output formatter.
    static ssize_t SQLiteFormatAppend(void* destString, const unsigned char* appendString, size_t length);
};
