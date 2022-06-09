#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>
#include <libstuff/SMultiSocketPool.h>

class SQLiteNode;
class BedrockCommand;

class SQLiteClusterMessenger {
  public:

    enum class WaitForReadyResult {
        OK,
        SHUTTING_DOWN,
        TIMEOUT,
        DISCONNECTED_IN,
        DISCONNECTED_OUT,
        UNSPECIFIED,
        POLL_ERROR,
    };

    SQLiteClusterMessenger(const shared_ptr<const SQLiteNode> node);

    // Attempts to make a TCP connection to the leader, and run the given command there, setting the appropriate
    // response from leader in the command, and marking it as complete if possible.
    // returns command->complete at the end of the function, this is true if the command was successfully completed on
    // leader, or if a fatal error occurred. This will be false if the command can be re-tried later (for instance, if
    // no connection to leader could be made).
    bool runOnLeader(BedrockCommand& command);

    // TODO: add informative comment
    vector<SData> runOnAll(const SData& command);

    // TODO: add informative comment
    bool runOnPeer(BedrockCommand& command, string peerName);

    // Set a timestamp by which we should give up on any pending commands. Once set, this is permanent. You will need a
    // new SQLiteClusterMessenger if you want to shutdown again.
    void shutdownBy(uint64_t shutdownTimestamp);

  private:
    // This takes a pollfd with either POLLIN or POLLOUT set, and waits for the socket to be ready to read or write,
    // respectively. It returns true if ready, or false if error or timeout. The timeout is specified as a timestamp in
    // microseconds.
    WaitForReadyResult waitForReady(pollfd& fdspec, uint64_t timeoutTimestamp);

    // This sets a command as a 500 and marks it as complete.
    static void setErrorResponse(BedrockCommand& command);

    // Checks if a command will cause the server to close this socket, indicating we can't reuse it.
    static bool commandWillCloseSocket(BedrockCommand& command);

    // TODO: writeme
    bool _sendCommandOnSocket(SHTTPSManager::Socket& socket, BedrockCommand& command);

    // TODO: writeme
    unique_ptr<SHTTPSManager::Socket> _getSocketForAddress(string address);

    const shared_ptr<const SQLiteNode> _node;

    // This is set to a timestamp when the server is shutting down so that we can abandon any commands that would
    // block that.
    atomic<uint64_t> _shutDownBy = 0;
    atomic_flag _shutdownSet = ATOMIC_FLAG_INIT;

    // For managing many connections to leader, we have a socket pool.
    unique_ptr<SMultiSocketPool> _socketPool;
};
