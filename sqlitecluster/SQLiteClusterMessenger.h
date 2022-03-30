#include <libstuff/libstuff.h>

class SQLiteNode;
class BedrockCommand;

class SQLiteClusterMessenger {
  public:
    SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node);

    // Attempts to make a TCP connection to the leader, and run the given command there, setting the appropriate
    // response from leader in the command, and marking it as complete if possible.
    // returns command->complete at the end of the function, this is true if the command was successfully completed on
    // leader, or if a fatal error occurred. This will be false if the command can be re-tried later (for instance, if
    // no connection to leader could be made).
    bool runOnLeader(BedrockCommand& command);

    void shutdownBy(uint64_t shutdownTimestamp);
    void reset();

  private:
    bool waitForReady(pollfd& fdspec, uint64_t timeoutTimestamp);

    void setErrorResponse(BedrockCommand& command);

    shared_ptr<SQLiteNode>& _node;

    atomic<uint64_t> _shutDownBy = 0;
};
