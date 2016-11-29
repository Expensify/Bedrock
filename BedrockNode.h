#pragma once
#include <sqlitecluster/SQLiteNode.h>

class BedrockServer;
class BedrockPlugin;
struct BedrockTester; // Defined in BedrockTester.h, but can't include else circular

class BedrockNode : public SQLiteNode {
  public:
    // Construct the base class
    BedrockNode(const SData& args, BedrockServer* server_);
    virtual ~BedrockNode();

    BedrockServer* server;

    bool isReadOnly();

    // STCPManager API: Socket management
    void postSelect(fd_map& fdm, uint64_t& nextActivity);

    // Handle an exception thrown by a plugin while peek/processing a command.
    void handleCommandException(SQLite& db, Command* command, const string& errorStr, bool wasProcessing);

    // SQLiteNode API: Command management
    virtual bool _peekCommand(SQLite& db, Command* command);
    virtual void _processCommand(SQLite& db, Command* command);
    virtual void _abortCommand(SQLite& db, Command* command);
    virtual void _cleanCommand(Command* command);
};
