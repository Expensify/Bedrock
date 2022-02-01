#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>

class SQLiteNode;
class BedrockCommand;

class SQLiteClusterMessenger : public SStandaloneHTTPSManager {
  public:
    SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node);
    Transaction* sendToLeader(BedrockCommand& command);
    virtual bool _onRecv(Transaction* transaction) override;

    virtual bool handleAllResponses() override { return true; }

  private:
    shared_ptr<SQLiteNode>& _node;

    // Map of transactions to their commands and escalation start times
    map<Transaction*, pair<BedrockCommand*, uint64_t>> _transactionCommands;
};
