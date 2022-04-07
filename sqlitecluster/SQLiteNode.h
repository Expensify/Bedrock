#pragma once
#include <libstuff/STCPNode.h>
#include <sqlitecluster/SQLite.h>
#include <sqlitecluster/SQLitePool.h>
#include <sqlitecluster/SQLiteSequentialNotifier.h>
#include <WallClockTimer.h>
#include <SynchronizedMap.h>

class SQLiteCommand;
class SQLiteServer;

// Distributed, leader/follower, failover, transactional DB cluster
class SQLiteNode : public STCPNode {
    // This exists to expose internal state to a test harness. It is not used otherwise.
    friend class SQLiteNodeTester;

  public:
    // Receive timeout for 'normal' SQLiteNode messages
    static const uint64_t SQL_NODE_DEFAULT_RECV_TIMEOUT;

    // Separate timeout for receiving and applying synchronization commits.
    static const uint64_t SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT;

    // Write consistencies available
    enum ConsistencyLevel {
        ASYNC,  // Fully asynchronous write, no follower approval required.
        ONE,    // Require exactly one approval (likely from a peer on the same LAN)
        QUORUM, // Require majority approval
        NUM_CONSISTENCY_LEVELS
    };
    static const string consistencyLevelNames[NUM_CONSISTENCY_LEVELS];

    // These are the possible states a transaction can be in.
    enum class CommitState {
        UNINITIALIZED,
        WAITING,
        COMMITTING,
        SUCCESS,
        FAILED
    };

    // Constructor/Destructor
    SQLiteNode(SQLiteServer& server, shared_ptr<SQLitePool> dbPool, const string& name, const string& host,
               const string& peerList, int priority, uint64_t firstTimeout, const string& version, const bool useParallelReplication = false,
               const string& commandPort = "localhost:8890");
    ~SQLiteNode();

    const vector<Peer*> initPeers(const string& peerList);

    // Simple Getters. See property definitions for details.
    State         getState()         { return _state; }
    int           getPriority()      { return _priority; }
    const string& getLeaderVersion() { return _leaderVersion; }
    const string& getVersion()       { return _version; }
    uint64_t      getCommitCount()   { return _db.getCommitCount(); }

    // Returns whether we're in the process of gracefully shutting down.
    bool gracefulShutdown() { return (_gracefulShutdownTimeout.alarmDuration != 0); }

    // True from when we call 'startCommit' until the commit has been sent to (and, if it required replication,
    // acknowledged by) peers.
    bool commitInProgress() { return (_commitState == CommitState::WAITING || _commitState == CommitState::COMMITTING); }

    // Returns true if the last commit was successful. If called while `commitInProgress` would return true, it returns
    // false.
    bool commitSucceeded() { return _commitState == CommitState::SUCCESS; }

    // Returns true if we're LEADING with enough FOLLOWERs to commit a quorum transaction. Not thread-safe to call
    // outside the sync thread.
    bool hasQuorum();

    // Call this if you want to shut down the node.
    void beginShutdown(uint64_t usToWait);

    // Call this to check if the node's completed shutting down.
    bool shutdownComplete();

    // Updates the internal state machine. Returns true if it wants immediate re-updating. Returns false to indicate it
    // would be a good idea for the caller to read any new commands or traffic from the network.
    bool update();

    // Return the state of the lead peer. Returns UNKNOWN if there is no leader, or if we are the leader.
    State leaderState() const;

    // Begins the process of committing a transaction on this SQLiteNode's database. When this returns,
    // commitInProgress() will return true until the commit completes.
    void startCommit(ConsistencyLevel consistency);

    // If we have a command that can't be handled on a follower, we can escalate it to the leader node. The SQLiteNode
    // takes ownership of the command until it receives a response from the follower. When the command completes, it will
    // be re-queued in the SQLiteServer (_server), but its `complete` field will be set to true.
    // If the 'forget' flag is set, we will not expect a response from leader for this command.
    void escalateCommand(unique_ptr<SQLiteCommand>&& command, bool forget = false);

    // This takes a completed command and sends the response back to the originating peer. If we're not the leader
    // node, or if this command doesn't have an `initiatingPeerID`, then calling this function is an error.
    void sendResponse(const SQLiteCommand& command);

    // This is a static function that can 'peek' a command initiated by a peer, but can be called by any thread.
    // Importantly for thread safety, this cannot depend on the current state of the cluster or a specific node.
    // Returns false if the node can't peek the command.
    static bool peekPeerCommand(shared_ptr<SQLiteNode>, SQLite& db, SQLiteCommand& command);

    // This exists so that the _server can inspect internal state for diagnostic purposes.
    list<string> getEscalatedCommandRequestMethodLines();

    // This will broadcast a message to all peers, or a specific peer.
    void broadcast(const SData& message, Peer* peer = nullptr);

    // Takes two string in the form of `host:port` (i.e., `www.expensify.com:80` or `127.0.0.1:443`) and creates a
    // similar string with the host from hostPart and the port from portPart .
    string replaceAddressPort(const string& hostPart, const string& portPart);

    SQLiteSequentialNotifier::RESULT waitForCommit(uint64_t commitNum);

  private:
    // STCPNode API: Peer handling framework functions
    void _onConnect(Peer* peer);
    void _onDisconnect(Peer* peer);
    void _onMESSAGE(Peer* peer, const SData& message);

    // This is a pool of DB handles that this node can use for any DB access it needs. Currently, it hands them out to
    // replication threads as required. It's passed in via the constructor.
    shared_ptr<SQLitePool> _dbPool;

    // Handle to the underlying database that we write to. This should also be passed to an SQLiteCore object that can
    // actually perform some action on the DB. When those action are complete, you can call SQLiteNode::startCommit()
    // to commit and replicate them.
    SQLite& _db;

    // Choose the best peer to synchronize from. If no other peer is logged in, or no logged in peer has a higher
    // commitCount that we do, this will return null.
    void _updateSyncPeer();
    Peer* _syncPeer;

    // Store the ID of the last transaction that we replicated to peers. Whenever we do an update, we will try and send
    // any new committed transactions to peers, and update this value.
    static uint64_t _lastSentTransactionID;

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
    mutable mutex _leadPeerMutex;

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

    // Our version string. Supplied by constructor.
    string _version;

    // leader's version string.
    string _leaderVersion;

    // The maximum number of seconds we'll allow before we force a quorum commit. This can be violated when commits
    // are performed outside of SQLiteNode, but we'll catch up the next time we do a commit.
    int _quorumCheckpointSeconds;

    // The timestamp of the (end of) the last quorum commit.
    uint64_t _lastQuorumTime;

    // Helper methods
    void _sendToPeer(Peer* peer, const SData& message);
    void _sendToAllPeers(const SData& message, bool subscribedOnly = false);
    void _changeState(State newState);

    // Queue a SYNCHRONIZE message based on the current state of the node, thread-safe, but you need to pass the
    // *correct* DB for the thread that's making the call (i.e., you can't use the node's internal DB from a worker
    // thread with a different DB object) - which is why this is static.
    static void _queueSynchronize(SQLiteNode* node, Peer* peer, SQLite& db, SData& response, bool sendAll);
    void _recvSynchronize(Peer* peer, const SData& message);
    void _reconnectPeer(Peer* peer);
    void _reconnectAll();
    bool _isQueuedCommandMapEmpty();
    bool _isNothingBlockingShutdown();
    bool _majoritySubscribed();

    // When we're a follower, we can escalate a command to the leader. When we do so, we store that command in the
    // following map of commandID to Command until the follower responds.
    SynchronizedMap<string, unique_ptr<SQLiteCommand>> _escalatedCommandMap;

    // Replicates any transactions that have been made on our database by other threads to peers.
    void _sendOutstandingTransactions(const set<uint64_t>& commitOnlyIDs = {});

    // The server object to which we'll pass incoming escalated commands.
    SQLiteServer& _server;

    // This is an integer that increments every time we change states. This is useful for responses to state changes
    // (i.e., approving standup) to verify that the messages we're receiving are relevant to the current state change,
    // and not stale responses to old changes.
    int _stateChangeCount;

    // Last time we recorded network stats.
    chrono::steady_clock::time_point _lastNetStatTime;

    // Handler for transaction messages.
    void handleBeginTransaction(SQLite& db, Peer* peer, const SData& message, bool wasConflict);
    void handlePrepareTransaction(SQLite& db, Peer* peer, const SData& message);
    int handleCommitTransaction(SQLite& db, Peer* peer, const uint64_t commandCommitCount, const string& commandCommitHash);
    void handleRollbackTransaction(SQLite& db, Peer* peer, const SData& message);
    
    // Legacy versions of the above functions for serial replication.
    void handleSerialBeginTransaction(Peer* peer, const SData& message);
    void handleSerialCommitTransaction(Peer* peer, const SData& message);
    void handleSerialRollbackTransaction(Peer* peer, const SData& message);

    WallClockTimer _syncTimer;
    atomic<uint64_t> _handledCommitCount;

    // State variable that indicates when the above threads should quit.
    atomic<bool> _replicationThreadsShouldExit;

    SQLiteSequentialNotifier _localCommitNotifier;
    SQLiteSequentialNotifier _leaderCommitNotifier;

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

    // Counter of the total number of currently active replication threads. This is used to let us know when all
    // threads have finished.
    atomic<int64_t> _replicationThreadCount;

    // Indicates whether this node is configured for parallel replication.
    const bool _useParallelReplication;

    // Monotonically increasing thread counter, used for thread IDs for logging purposes.
    static atomic<int64_t> _currentCommandThreadID;

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

    AutoTimer _multiReplicationThreadSpawn;
    AutoTimer _legacyReplication;
    AutoTimer _onMessageTimer;
    AutoTimer _escalateTimer;

    // A string representing an address (i.e., `127.0.0.1:80`) where this server accepts commands. I.e., "the command
    // port".
    const string _commandAddress;
};
