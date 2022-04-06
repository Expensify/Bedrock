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

    // Set a timestamp by which we should give up on any pending commands.
    void shutdownBy(uint64_t shutdownTimestamp);

    // Reset to not be shutting down.
    void reset();

  private:
    // This takes a pollfd with either POLLIN or POLLOUT set, and waits for the socket to be ready to read or write,
    // respectively. It returns true if ready, or false if error or timeout. The timeout is specified as a timestamp in
    // microseconds.
    bool waitForReady(pollfd& fdspec, uint64_t timeoutTimestamp);

    // This sets a command as a 500 and marks it as complete.
    void setErrorResponse(BedrockCommand& command);

    shared_ptr<SQLiteNode>& _node;

    // This is set to a timestamp when the server is shutting down so that we can abandon any commands that would
    // block that.
    atomic<uint64_t> _shutDownBy = 0;
};
