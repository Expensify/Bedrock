#pragma once
#include <libstuff/libstuff.h>
#include <libstuff/SSynchronizedQueue.h>
#include <libstuff/STCPManager.h>
#include <sqlitecluster/SQLite.h>
#include <sqlitecluster/SQLitePool.h>
#include <sqlitecluster/SQLiteSequentialNotifier.h>
#include <WallClockTimer.h>
#include <SynchronizedMap.h>

// This file is long and complex. For each nested sub-structure (I.e., classes inside classes) we have attempted to
// arrange things as such:
// For each puplic private block:
// 1. classes/structs/type definitions
// 2. static members
// 3. static methods
// 4. const methods
// 5. non-const methods
// 6. const instance members
// 7. non-const instance members.
// 8. In each of these sections, things should be alphabetized.

// Diagnostic class for timing what fraction of time happens in certain blocks.
// TODO: Move out of SQLiteNode.
class AutoTimer {
  public:
    AutoTimer(string name);
    void start();
    void stop();

  private:
    string _name;
    chrono::steady_clock::time_point _intervalStart;
    chrono::steady_clock::time_point _instanceStart;
    chrono::steady_clock::duration _countedTime;
};

class AutoTimerTime {
  public:
    AutoTimerTime(AutoTimer& t);
    ~AutoTimerTime();

  private:
    AutoTimer& _t;
};

class SQLiteCommand;
class SQLiteServer;

// Distributed, leader/follower, failover, transactional DB cluster
class SQLiteNode : public STCPManager {
    // This exists to expose internal state to a test harness. It is not used otherwise.
    friend class SQLiteNodeTest;
    friend class SQLiteNodeTester;

  public:
    // These are the possible states a transaction can be in.
    enum class CommitState {
        UNINITIALIZED,
        WAITING,
        COMMITTING,
        SUCCESS,
        FAILED
    };

    // Write consistencies available
    enum ConsistencyLevel {
        ASYNC,  // Fully asynchronous write, no follower approval required.
        ONE,    // Require exactly one approval (likely from a peer on the same LAN)
        QUORUM, // Require majority approval
        NUM_CONSISTENCY_LEVELS
    };

    // Possible states of a node in a DB cluster
    // Before `Peer` because `Peer` references it.
    enum State {
        UNKNOWN,
        SEARCHING,     // Searching for peers
        SYNCHRONIZING, // Synchronizing with highest priority peer
        WAITING,       // Waiting for an opportunity to leader or follower
        STANDINGUP,    // Taking over leadership
        LEADING,       // Acting as leader node
        STANDINGDOWN,  // Giving up leader role
        SUBSCRIBING,   // Preparing to follow the leader
        FOLLOWING      // Following the leader node
    };

    // Represents a single peer in the database cluster
    class Peer {
        // This allows direct access to the socket from the node object that should actually be managing peer
        // connections, which should always be handled by a single thread, and thus safe. Ideally, this isn't required,
        // but for the time being, the amount of refactoring required to fix that is too high.
        friend class SQLiteNode;

      public:
        // Possible responses from a peer.
        enum class Response {
            NONE,
            APPROVE,
            DENY
        };

        // Get a string name for a Response object.
        static string responseName(Response response);

        // Atomically get commit and hash.
        void getCommit(uint64_t& count, string& hashString) const;

        // Gets an STable representation of this peer's current state in order to display status info.
        STable getData() const;

        // Returns true if there's an active connection to this Peer.
        bool connected() const;

        Peer(const string& name_, const string& host_, const STable& params_, uint64_t id_);
        ~Peer();

        // Reset a peer, as if disconnected and starting the connection over.
        void reset();

        // Send a message to this peer. Thread-safe.
        void sendMessage(const SData& message);

        // Atomically set commit and hash.
        void setCommit(uint64_t count, const string& hashString);

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

        // An address on which this peer can accept commands.
        atomic<string> commandAddress;
        atomic<int> failedConnections;
        atomic<uint64_t> latency;
        atomic<bool> loggedIn;
        atomic<uint64_t> nextReconnect;
        atomic<int> priority;
        atomic<State> state;
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
        Socket* socket = nullptr;
    };

    // This is a static function that can 'peek' a command initiated by a peer, but can be called by any thread.
    // Importantly for thread safety, this cannot depend on the current state of the cluster or a specific node.
    // Returns false if the node can't peek the command.
    [[deprecated("Use HTTP escalation")]]
    static bool peekPeerCommand(shared_ptr<SQLiteNode>, SQLite& db, SQLiteCommand& command);

    // Get and SQLiteNode State from it's name.
    static State stateFromName(const string& name);

    // Return the string representing an SQLiteNode State
    static const string& stateName(State state);

    // True from when we call 'startCommit' until the commit has been sent to (and, if it required replication,
    // acknowledged by) peers.
    bool commitInProgress() const;

    // Returns true if the last commit was successful. If called while `commitInProgress` would return true, it returns
    // false.
    bool commitSucceeded() const;

    uint64_t getCommitCount() const;

    const string& getLeaderVersion() const;

    list<STable> getPeerInfo() const;

    int getPriority() const;

    State getState() const;

    // Returns true if we're LEADING with enough FOLLOWERs to commit a quorum transaction. Not thread-safe to call
    // outside the sync thread.
    bool hasQuorum() const;

    // Return the command address of the current leader, if there is one (empty string otherwise).
    string leaderCommandAddress() const;

    // Return the state of the lead peer. Returns UNKNOWN if there is no leader, or if we are the leader.
    State leaderState() const;

    // Tell the node a commit has been made by another thread, so that we can interrupt our poll loop if we're waiting
    // for data, and send the new commit.
    void notifyCommit() const;

    // Prepare a set of sockets to wait for read/write.
    void prePoll(fd_map& fdm) const;

    // Call this if you want to shut down the node.
    void beginShutdown(uint64_t usToWait);

    // This will broadcast a message to all peers, or a specific peer.
    [[deprecated("Use HTTP escalation")]]
    void broadcast(const SData& message, Peer* peer = nullptr);

    // If we have a command that can't be handled on a follower, we can escalate it to the leader node. The SQLiteNode
    // takes ownership of the command until it receives a response from the follower. When the command completes, it will
    // be re-queued in the SQLiteServer (_server), but its `complete` field will be set to true.
    // If the 'forget' flag is set, we will not expect a response from leader for this command.
    [[deprecated("Use HTTP escalation")]]
    void escalateCommand(unique_ptr<SQLiteCommand>&& command, bool forget = false);

    // Handle any read/write events that occurred.
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // This takes a completed command and sends the response back to the originating peer. If we're not the leader
    // node, or if this command doesn't have an `initiatingPeerID`, then calling this function is an error.
    [[deprecated("Use HTTP escalation")]]
    void sendResponse(const SQLiteCommand& command);

    // Call this to check if the node's completed shutting down.
    bool shutdownComplete();

    // Constructor/Destructor
    SQLiteNode(SQLiteServer& server, shared_ptr<SQLitePool> dbPool, const string& name, const string& host,
               const string& peerList, int priority, uint64_t firstTimeout, const string& version,
               const string& commandPort = "localhost:8890");
    ~SQLiteNode();

    // Begins the process of committing a transaction on this SQLiteNode's database. When this returns,
    // commitInProgress() will return true until the commit completes.
    void startCommit(ConsistencyLevel consistency);

    // Updates the internal state machine. Returns true if it wants immediate re-updating. Returns false to indicate it
    // would be a good idea for the caller to read any new commands or traffic from the network.
    bool update();

  private:
    // Utility class that can decrement _replicationThreadCount when objects go out of scope.
    template <typename CounterType>
    class ScopedDecrement {
      public:
        ScopedDecrement(CounterType& counter) : _counter(counter) {} 
        ~ScopedDecrement() {
            --_counter;
        }
      private:
        CounterType& _counter;
    };

    // The names of each of the consistency levels defined in `ConsistencyLevel` as strings. This is only actually used
    // for logging.
    static const string CONSISTENCY_LEVEL_NAMES[NUM_CONSISTENCY_LEVELS];

    // Monotonically increasing thread counter, used for thread IDs for logging purposes.
    static atomic<int64_t> currentReplicateThreadID;

    // Receive timeout for 'normal' SQLiteNode messages
    static const uint64_t SQL_NODE_DEFAULT_RECV_TIMEOUT;

    // Separate timeout for receiving and applying synchronization commits.
    static const uint64_t SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT;

    static const vector<Peer*> _initPeers(const string& peerList);

    // Queue a SYNCHRONIZE message based on the current state of the node, thread-safe, but you need to pass the
    // *correct* DB for the thread that's making the call (i.e., you can't use the node's internal DB from a worker
    // thread with a different DB object) - which is why this is static.
    static void _queueSynchronize(SQLiteNode* node, Peer* peer, SQLite& db, SData& response, bool sendAll);

    // This is the main replication loop that's run in the replication threads. It's instantiated in a new thread for
    // each new relevant replication command received by the sync thread.
    //
    // There are three commands we currently handle here BEGIN_TRANSACTION, ROLLBACK_TRANSACTION, and
    // COMMIT_TRANSACTION.
    // ROLLBACK_TRANSACTION and COMMIT_TRANSACTION are trivial, they record the new highest commit number from LEADER,
    // or instruct the node to go SEARCHING and reconnect if a distributed ROLLBACK happens.
    //
    // BEGIN_TRANSACTION is where the interesting case is. This starts all transactions in parallel, and then waits
    // until each previous transaction is committed such that the final commit order matches LEADER. It also handles
    // commit conflicts by re-running the transaction from the beginning. Most of the logic for making sure
    // transactions are ordered correctly is done in `SQLiteSequentialNotifier`, which is worth reading.
    //
    // This thread exits on completion of handling the command or when node._replicationThreadsShouldExit is set,
    // which happens when a node stops FOLLOWING.
    static void replicate(SQLiteNode& node, Peer* peer, SData command, size_t sqlitePoolIndex);

    // Returns whether we're in the process of gracefully shutting down.
    bool _gracefulShutdown() const;

    bool _isNothingBlockingShutdown() const;
    bool _majoritySubscribed() const;

    // Returns a peer by it's ID. If the ID is invalid, returns nullptr.
    Peer* _getPeerByID(uint64_t id) const;

    // Inverse of the above function. If the peer is not found, returns 0.
    uint64_t _getIDByPeer(Peer* peer) const;

    Socket* _acceptSocket();

    // Called when we first establish a connection with a new peer
    void _onConnect(Peer* peer);

    // Called when we lose connection with a peer
    void _onDisconnect(Peer* peer);

    // Called when the peer sends us a message; throw an SException to reconnect.
    void _onMESSAGE(Peer* peer, const SData& message);

    // Choose the best peer to synchronize from. If no other peer is logged in, or no logged in peer has a higher
    // commitCount that we do, this will return null.
    void _updateSyncPeer();

    // Helper methods
    void _sendToPeer(Peer* peer, const SData& message);
    void _sendToAllPeers(const SData& message, bool subscribedOnly = false);
    void _changeState(State newState);

    void _recvSynchronize(Peer* peer, const SData& message);
    void _reconnectPeer(Peer* peer);
    void _reconnectAll();

    // Replicates any transactions that have been made on our database by other threads to peers.
    void _sendOutstandingTransactions(const set<uint64_t>& commitOnlyIDs = {});

    // Handler for transaction messages.
    void handleBeginTransaction(SQLite& db, Peer* peer, const SData& message, bool wasConflict);
    void handlePrepareTransaction(SQLite& db, Peer* peer, const SData& message);
    int handleCommitTransaction(SQLite& db, Peer* peer, const uint64_t commandCommitCount, const string& commandCommitHash);
    void handleRollbackTransaction(SQLite& db, Peer* peer, const SData& message);
    
    // Helper functions
    void _sendPING(Peer* peer);

    const string _name;
    const vector<Peer*> _peerList;
    const uint64_t _recvTimeout;

    // A bunch of private properties.
    list<STCPManager::Socket*> _socketList;
    // TODO:: These are redundant and probably contain the same thing. Or one is empty? Either way it's confusing.
    list<Socket*> _acceptedSocketList;

    unique_ptr<Port> port;

    // Store the ID of the last transaction that we replicated to peers. Whenever we do an update, we will try and send
    // any new committed transactions to peers, and update this value.
    uint64_t _lastSentTransactionID;

    // This is a pool of DB handles that this node can use for any DB access it needs. Currently, it hands them out to
    // replication threads as required. It's passed in via the constructor.
    shared_ptr<SQLitePool> _dbPool;

    // Handle to the underlying database that we write to. This should also be passed to an SQLiteCore object that can
    // actually perform some action on the DB. When those action are complete, you can call SQLiteNode::startCommit()
    // to commit and replicate them.
    SQLite& _db;

    Peer* _syncPeer;

    // Our priority, with respect to other nodes in the cluster. This is passed in to our constructor. The node with
    // the highest priority in the cluster will attempt to become the leader.
    atomic<int> _priority;

    // When the node starts, it is not ready to serve requests without first connecting to the other nodes, and checking
    // to make sure it's up-to-date. Store the configured priority here and use "-1" until we're ready to fully join the cluster.
    int _originalPriority;

    // Our current State.
    atomic<State> _state;
    
    // Pointer to the peer that is the leader. Null if we're the leader, or if we don't have a leader yet.
    atomic<Peer*> _leadPeer;

    // There's a mutex here to lock around changes to this, or any complex operations that expect leader to remain
    // unchanged throughout, notably, _sendToPeer. This is sort of a mess, but replication threads need to send
    // acknowledgments to the lead peer, but the main sync loop can update that at any time.
    mutable shared_mutex _leadPeerMutex;

    // Timestamp that, if we pass with no activity, we'll give up on our current state, and start over from SEARCHING.
    uint64_t _stateTimeout;

    // This is the current CommitState we're in with regard to committing a transaction. It is `UNINITIALIZED` from
    // startup until a transaction is started.
    CommitState _commitState;

    // The write consistency requested for the current in-progress commit.
    ConsistencyLevel _commitConsistency;

    // Stopwatch to track if we're going to give up on gracefully shutting down and force it.
    SStopwatch _gracefulShutdownTimeout;

    // Stopwatch to track if we're giving up on the server preventing a standdown.
    SStopwatch _standDownTimeOut;

    // leader's version string.
    string _leaderVersion;

    // The maximum number of seconds we'll allow before we force a quorum commit. This can be violated when commits
    // are performed outside of SQLiteNode, but we'll catch up the next time we do a commit.
    int _quorumCheckpointSeconds;

    // The timestamp of the (end of) the last quorum commit.
    uint64_t _lastQuorumTime;

    // When we're a follower, we can escalate a command to the leader. When we do so, we store that command in the
    // following map of commandID to Command until the follower responds.
    SynchronizedMap<string, unique_ptr<SQLiteCommand>> _escalatedCommandMap;

    // The server object to which we'll pass incoming escalated commands.
    SQLiteServer& _server;

    // This is an integer that increments every time we change states. This is useful for responses to state changes
    // (i.e., approving standup) to verify that the messages we're receiving are relevant to the current state change,
    // and not stale responses to old changes.
    int _stateChangeCount;

    // Last time we recorded network stats.
    chrono::steady_clock::time_point _lastNetStatTime;

    WallClockTimer _syncTimer;
    atomic<uint64_t> _handledCommitCount;

    // State variable that indicates when the above threads should quit.
    atomic<bool> _replicationThreadsShouldExit;

    SQLiteSequentialNotifier _localCommitNotifier;
    SQLiteSequentialNotifier _leaderCommitNotifier;

    // Counter of the total number of currently active replication threads. This is used to let us know when all
    // threads have finished.
    atomic<int64_t> _replicationThreadCount;

    // A string representing an address (i.e., `127.0.0.1:80`) where this server accepts commands. I.e., "the command
    // port".
    const string _commandAddress;

    // This is just here to allow `poll` to get interrupted when there are new commits to send. We don't want followers
    // to wait up to a full second for them.
    mutable SSynchronizedQueue<bool> _commitsToSend;

    // Our version string. Supplied by constructor.
    const string _version;
};

// serialization for Responses.
ostream& operator<<(ostream& os, const atomic<SQLiteNode::Peer::Response>& response);
