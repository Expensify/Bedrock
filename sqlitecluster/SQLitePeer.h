#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>

// Represents a single peer in the database cluster
class SQLitePeer {
  public:
    // Possible responses from a peer.
    enum class Response {
        NONE,
        APPROVE,
        DENY,
        ABSTAIN
    };

    enum class PeerPostPollStatus {
        OK,
        JUST_CONNECTED,
        SOCKET_ERROR,
        SOCKET_CLOSED,
    };

    // Get a string name for a Response object.
    static string responseName(Response response);

    // Atomically get commit and hash.
    void getCommit(uint64_t& count, string& hashString) const;

    // Gets an STable representation of this peer's current state in order to display status info.
    STable getData() const;

    // Returns true if there's an active connection to this Peer.
    bool connected() const;

    // The most recent receive time, in microseconds since the epoch.
    uint64_t lastRecvTime() const;

    // The most recent send time, in microseconds since the epoch.
    uint64_t lastSendTime() const;

    void prePoll(fd_map& fdm) const;

    // Reset a peer, as if disconnected and starting the connection over.
    void reset();

    // Pops a message off the *front* of the receive buffer and returns it.
    // If there are no messages, throws `std::out_of_range`.
    SData popMessage();

    PeerPostPollStatus postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Send a message to this peer. Thread-safe.
    void sendMessage(const SData& message);

    // Atomically set commit and hash.
    void setCommit(uint64_t count, const string& hashString);

    // Sets the socket to the new socket, but will fail if the socket is already set unless onlyIfNull is false.
    // returns whether or not the socket was actually set.
    bool setSocket(STCPManager::Socket* newSocket, bool onlyIfNull = true);

    void shutdownSocket();

    SQLitePeer(const string& name_, const string& host_, const STable& params_, uint64_t id_);
    ~SQLitePeer();

    // This is const because it's public, and we don't want it to be changed outside of this class, as it needs to
    // be synchronized with `hash`. However, it's often useful just as it is, so we expose it like this and update
    // it with `const_cast`. `hash` is only used in few places, so is private, and can only be accessed with
    // `getCommit`, thus reducing the risk of anyone getting out-of-sync commitCount and hash.
    const atomic<uint64_t> commitCount;

    const string host;
    const uint64_t id;
    const string name;
    const STable params;
    const bool permaFollower;

    // An address on which this peer can accept commands. (a.k.a. "private command port")
    atomic<string> commandAddress;
    atomic<uint64_t> latency;
    atomic<bool> loggedIn;
    atomic<uint64_t> nextReconnect;
    atomic<int> priority;
    atomic<SQLiteNodeState> state;
    atomic<Response> standupResponse;
    atomic<bool> subscribed;
    atomic<Response> transactionResponse;
    atomic<string> version;

  private:
    // For initializing the permafollower value from the params list.
    static bool isPermafollower(const STable& params);

    // The hash corresponding to commitCount.
    atomic<string> hash;

    // Mutex for locking around non-atomic member access (for set/getCommit, accessing socket, etc).
    mutable recursive_mutex peerMutex;

    // Not named with an underscore because it's only sort-of private (see friend class declaration above).
    STCPManager::Socket* socket = nullptr;
};

// serialization for Responses.
ostream& operator<<(ostream& os, const atomic<SQLitePeer::Response>& response);
