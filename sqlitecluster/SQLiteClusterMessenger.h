#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>
#include <libstuff/SMultiHostSocketPool.h>

class SQLiteNode;
class BedrockCommand;

class SQLiteClusterMessenger {
public:

    enum class WaitForReadyResult
    {
        OK,
        SHUTTING_DOWN,
        TIMEOUT,
        DISCONNECTED_IN,
        DISCONNECTED_OUT,
        UNSPECIFIED,
        POLL_ERROR,
    };

    SQLiteClusterMessenger(const shared_ptr<const SQLiteNode>& node);

    // Attempts to make a TCP connection to a peer, that could be the leader or not, and run the given command there,
    //  setting the appropriate response from the peer in the command, and marking it as complete if possible.
    // Returns command->complete at the end of the function, this is true if the command was successfully completed on
    // the peer, or if a fatal error occurred. This will be false if the command can be re-tried later (for instance, if
    // no connection to leader could be made).
    bool runOnPeer(BedrockCommand& command, bool runOnLeader);

    // Attempts to run command on every peer. This is done in threads, so the
    // order in which the peers run the command is not deterministic. Returns a
    // vector of response objects from each command after they are run. It is
    // up to the caller to inspect the responses and determine which if any of
    // the commands to retry.
    vector<SData> runOnAll(const SData& command);

    // Attempts to make a TCP connection to a specified peer, and run the given
    // command there, setting the appropriate response from the peer in the
    // command, and marking it as complete if possible. Returns
    // command->complete at the end of the function, this is true if the
    // command was successfully completed, or if a fatal error occurred. Unlike
    // runOnLeader, if the command fails for any reason, command.complete will
    // be set to true and not retried. It is up to the caller to determine how
    // to handle the failure.
    bool runOnPeer(BedrockCommand& command, const string& peerName);

private:
    // This takes a pollfd with either POLLIN or POLLOUT set, and waits for the socket to be ready to read or write,
    // respectively. It returns true if ready, or false if error or timeout. The timeout is specified as a timestamp in
    // microseconds.
    WaitForReadyResult waitForReady(pollfd& fdspec, uint64_t timeoutTimestamp) const;

    // This sets a command as a 500 and marks it as complete.
    static void setErrorResponse(BedrockCommand& command);

    // Checks if a command will cause the server to close this socket, indicating we can't reuse it.
    static bool commandWillCloseSocket(BedrockCommand& command);

    // Sends command to the host associated with socket. Returns true if the
    // command was sent successfully (command.complete will be set to true in
    // that case), false otherwise.
    bool _sendCommandOnSocket(SHTTPSManager::Socket& socket, BedrockCommand& command) const;

    // Parses the address to confirm it is valid, then requests a socket from
    // the socket pool. Returns either a pointer to the socket or nullptr if
    // there is an error.
    unique_ptr<SHTTPSManager::Socket> _getSocketForAddress(const string& address);

    const shared_ptr<const SQLiteNode> _node;

    // For managing many connections to leader, we have a socket pool.
    SMultiHostSocketPool _socketPool;
};
