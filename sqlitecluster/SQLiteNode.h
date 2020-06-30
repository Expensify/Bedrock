#pragma once
#include "SQLite.h"
#include "WallClockTimer.h"
#include "libstuff/SSynchronizedQueue.h"
class SQLiteCommand;
class SQLiteServer;

class NotifyAtValue {
  public:
    NotifyAtValue() : _value(0) {}
    void waitFor(uint64_t value);
    void notifyThrough(uint64_t value);
    void notifyAll();

  private:
    mutex _m;
    map <uint64_t, pair<shared_ptr<mutex>, shared_ptr<condition_variable>>> pending;
    uint64_t _value;
};

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
    SQLiteNode(SQLiteServer& server, SQLitePool& dbPool, const string& name, const string& host,
               const string& peerList, int priority, uint64_t firstTimeout, const string& version);
    ~SQLiteNode();

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
    static bool peekPeerCommand(SQLiteNode* node, SQLite& db, SQLiteCommand& command);

    // This is a static and thus *global* indicator of whether or not we have transactions that need replicating to
    // peers. It's global because it can be set by any thread. Because SQLite can run in parallel, we can have multiple
    // threads making commits to the database, and they communicate that to the node via this flag.
    static atomic<bool> unsentTransactions;

    // This exists so that the _server can inspect internal state for diagnostic purposes.
    list<string> getEscalatedCommandRequestMethodLines();

    // This mutex is exposed publicly so that others (particularly, the _server) can atomically act on the current
    // state of the node. When working with this and SQLite::g_commitLock, the correct order of acquisition is always:
    // 1. stateMutex
    // 2. SQLite::g_commitLock
    shared_timed_mutex stateMutex;

    // This will broadcast a message to all peers, or a specific peer.
    void broadcast(const SData& message, Peer* peer = nullptr);

  private:
    // STCPNode API: Peer handling framework functions
    void _onConnect(Peer* peer);
    void _onDisconnect(Peer* peer);
    void _onMESSAGE(Peer* peer, const SData& message);

    SQLitePool& _dbPool;

    // Handle to the underlying database that we write to. This should also be passed to an SQLiteCore object that can
    // actually perform some action on the DB. When those action are complete, you can call SQLiteNode::startCommit()
    // to commit and replicate them.
    SQLite& _db;

    // We have a separate list of DB handles used solely for replication, so that we can do replication in parallel
    // threads without causing conflicts in the journal.
    list<SQLite> _replicationDBs;

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

    // Queue a SYNCHRONIZE message based on the current state of the node.
    void _queueSynchronize(Peer* peer, SData& response, bool sendAll);

    // Queue a SYNCHRONIZE message based on pre-computed state of the node. This version is thread-safe.
    static void _queueSynchronizeStateless(const STable& params, const string& name, const string& peerName, State _state, uint64_t targetCommit, SQLite& db, SData& response, bool sendAll);
    void _recvSynchronize(Peer* peer, const SData& message);
    void _reconnectPeer(Peer* peer);
    void _reconnectAll();
    bool _isQueuedCommandMapEmpty();
    bool _isNothingBlockingShutdown();
    bool _majoritySubscribed();

    // When we're a follower, we can escalate a command to the leader. When we do so, we store that command in the
    // following map of commandID to Command until the follower responds.
    map<string, unique_ptr<SQLiteCommand>> _escalatedCommandMap;

    // Replicates any transactions that have been made on our database by other threads to peers.
    void _sendOutstandingTransactions();

    // The server object to which we'll pass incoming escalated commands.
    SQLiteServer& _server;

    // This is an integer that increments every time we change states. This is useful for responses to state changes
    // (i.e., approving standup) to verify that the messages we're receiving are relevant to the current state change,
    // and not stale responses to old changes.
    int _stateChangeCount;

    // Last time we recorded network stats.
    chrono::steady_clock::time_point _lastNetStatTime;

    // Handler for transaction messages.
    void handleBeginTransaction(SQLite& db, Peer* peer, const SData& message);
    void handleCommitTransaction(SQLite& db, Peer* peer, const uint64_t commandCommitCount, const string& commandCommitHash);
    void handleRollbackTransaction(SQLite& db, Peer* peer, const SData& message);

    WallClockTimer _syncTimer;
    atomic<uint64_t> _handledCommitCount;

    // Count of current number of working threads, we keep track of this so we can tell when they've finished.
    atomic<int64_t> _replicationThreads;

    // State variable that indicates when the above threads should quit.
    atomic<bool> _replicationThreadsShouldExit;

    // Mutex that guards the map of hashes for transactions currently being replicated.
    mutex _replicationHashMutex;
    set<string> _replicationHashesToCommit;
    set<string> _replicationHashesToRollback;

    mutex _replicationMutex;
    NotifyAtValue _dbNotifier;
    NotifyAtValue _commitNotifier;

    // Replication thread main body.
    static void replicate(SQLiteNode& node, Peer* peer, SData command);
};
