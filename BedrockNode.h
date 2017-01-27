#pragma once
#include <sqlitecluster/SQLiteNode.h>

class BedrockServer;
class BedrockPlugin;
struct BedrockTester; // Defined in BedrockTester.h, but can't include else circular

class BedrockNode : public SQLiteNode {
  public:
    // Construct the base class
    BedrockNode(const SData& args, int threadId, int threadCount, BedrockServer* server_);
    virtual ~BedrockNode();

    BedrockServer* server;

    bool isWorker();

    void setSyncNode(BedrockNode* node);

    bool dbReady();

    // STCPManager API: Socket management
    void postSelect(fd_map& fdm, uint64_t& nextActivity);

    // Handle an exception thrown by a plugin while peek/processing a command.
    void handleCommandException(SQLite& db, Command* command, const string& errorStr, bool wasProcessing);

    // SQLiteNode API: Command management
    virtual bool _peekCommand(SQLite& db, Command* command);
    virtual void _processCommand(SQLite& db, Command* command);
    virtual void _abortCommand(SQLite& db, Command* command);
    virtual void _cleanCommand(Command* command);

  protected:
    virtual void _setState(SQLCState state);
    virtual void _conflictHandler(Command* command);

  private:
    // If we're the sync node, we keep track of whether our database is ready to use.
    bool _dbReady = false;

    // If we're a worker node, we keep track of a sync node, to know when his database is ready to use.
    BedrockNode* _syncNode = 0;
};
