#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>

class SQLiteNode;
class BedrockCommand;

class SQLiteClusterMessenger : public SStandaloneHTTPSManager {
  public:
    SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node);
    Transaction* sendToLeader(BedrockCommand& command);

  private:
    shared_ptr<SQLiteNode>& _node;
};
