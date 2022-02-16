#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>

class SQLiteNode;
class BedrockCommand;

class SQLiteClusterMessenger : public SStandaloneHTTPSManager {
  public:
    SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node);
    bool sendToLeader(BedrockCommand& command);
    virtual bool _onRecv(Transaction* transaction) override;

    virtual bool handleAllResponses() override;

  private:
    shared_ptr<SQLiteNode>& _node;

    // Map of transactions to their commands and escalation start times, and a mutex to use for protecting access to it.
    mutex _transactionCommandMutex;
    map<Transaction*, pair<BedrockCommand*, uint64_t>> _transactionCommands;
};
