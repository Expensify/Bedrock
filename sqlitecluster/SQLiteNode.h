#pragma once
#include "SQLite.h"

// Forward declarations. SQLiteNode passes SQLiteCommands received from peers to an SQLiteServer.
class SQLiteCommand;
class SQLiteServer;

// Distributed, master/slave, failover, transactional DB cluster
class SQLiteNode : public STCPNode {
    // This exists to expose internal state to a test harness. It is not used otherwise.
    friend class SQLiteNodeTester;

  public:

    // Possible states of a node in a DB cluster
    enum State {
        SEARCHING,     // Searching for peers
        SYNCHRONIZING, // Synchronizing with highest priority peer
        WAITING,       // Waiting for an opportunity to master or slave
        STANDINGUP,    // Taking over master-ship
        MASTERING,     // Acting as master node
        STANDINGDOWN,  // Giving up master role
        SUBSCRIBING,   // Preparing to slave to the master
        SLAVING,       // Slaving to the master node
        NUM_STATES
    };
    static const string stateNames[NUM_STATES];

    // Write consistencies available
    enum ConsistencyLevel {
        ASYNC,  // Fully asynchronous write, no slave approval required.
        ONE,    // Require exactly one approval (likely from a peer on the same LAN)
        QUORUM, // Require majority approval
        NUM_CONSISTENCY_LEVELS
    };
    static const string consistencyLevelNames[NUM_CONSISTENCY_LEVELS];

    // These are the possible states a transaction can be in.
    enum class CommitState {
        UNKNOWN,
        WAITING,
        COMMITTING,
        SUCCESS,
        FAILED
    };

    // Constructor/Destructor
    SQLiteNode(SQLiteServer& server, SQLite& db, const string& name, const string& host, const string& peerList,
               int priority, uint64_t firstTimeout, const string& version, int quorumCheckpoint = 0);
    ~SQLiteNode();

    // Simple Getters. Descriptions of the variables they return will be with their definitions.
    State         getState()         { return _state; }
    int           getPriority()      { return _priority; }
    const string& getMasterVersion() { return _masterVersion; }
    const string& getVersion()       { return _version; }

    // Returns whether the node is ready to process traffic.
    bool ready() { return (_state == MASTERING || _state == SLAVING); }

    // Returns whether we're in the process of gracefully shutting down.
    bool gracefulShutdown() { return (_gracefulShutdownTimeout.alarmDuration != 0); }

    // True from when we call 'startCommit' until the commit has been sent to (and, if it required replication,
    // acknowledged by) peers.
    bool commitInProgress() { return (_commitState == CommitState::WAITING || _commitState == CommitState::COMMITTING); }

    // Returns true if the last commit was successful. If called while `commitInProgress` would return true, it returns
    // false.
    bool commitSucceeded() { return _commitState == CommitState::SUCCESS; }

    // Call this if you want to shut down the node.
    void beginShutdown();

    // Call this to check if the node's completed shutting down.
    bool shutdownComplete();

    // Updates the internal state machine. Returns true if it wants immediate re-updating. Returns false to indicate it
    // would be a good idea for the caller to read any new commands or traffic from the network.
    bool update();

    // Begins the process of committing a transaction on this SQLiteNode's database. When this returns,
    // commitInProgress() will return true until the commit completes.
    void startCommit(ConsistencyLevel consistency);

    // If we have a command that can't be handled on a slave, we can escalate it to the master node. The SQLiteNode
    // takes ownership of the command until it receives a response from the slave. When the command completes, it will
    // be re-queued in the SQLiteServer, but its `complete` field will be set to true.
    void escalateCommand(SQLiteCommand&& command);

    // This takes a completed command and sends the response back to the originating peer. If this command doesn't have
    // an `initiatingPeerID`, then it's an error to pass it to this function, or if we're not the master node, it's an
    // error to call this method.
    void sendResponse(const SQLiteCommand& command);

    // This is a static and thus *global* indicator of whether or not we have transactions that need replicating to
    // peers. It's global because it can be set by any thread. Because SQLite can run in parallel, we can have multiple
    // threads making commits to the database, and they communicate that data to us via this variable.
    static atomic<bool> unsentTransactions;

  private:
    // STCPNode API: Peer handling framework functions
    void _onConnect(Peer* peer);
    void _onDisconnect(Peer* peer);
    void _onMESSAGE(Peer* peer, const SData& message);

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
    // the highest priority in the cluster will attempt to become the MASTER.
    int _priority;

    // Our current State.
    State _state;
    
    // Pointer to the peer that is the master. Null if we're the master, or if we don't have a master yet.
    Peer* _masterPeer;

    // Timestamp that, if we pass with no activity, we'll give up on our current state, and start over from SEARCHING.
    uint64_t _stateTimeout;

    // This is the current CommitState we're in with regard to committing a transaction.
    CommitState _commitState;

    // The write consistency requested for the current in-progress commit.
    ConsistencyLevel _commitConsistency;

    // Stopwatch to track if we're going to give up on gracefully shutting down and force it.
    SStopwatch _gracefulShutdownTimeout;

    // Our version string. Supplied by constructor.
    string _version;

    // Master's version string.
    string _masterVersion;

    // The maximum number of commits we'll allow before we force a quorum commit. This can be violated when commits
    // are performed outside of SQLiteNode, but we'll catch up the next time we do a commit.
    int _quorumCheckpoint;

    // The number of commits we've actually done since the last quorum command.
    int _commitsSinceCheckpoint;

    // Helper methods
    void _sendToPeer(Peer* peer, const SData& message);
    void _sendToAllPeers(const SData& message, bool subscribedOnly = false);
    void _changeState(State newState);
    void _queueSynchronize(Peer* peer, SData& response, bool sendAll);
    void _recvSynchronize(Peer* peer, const SData& message);
    void _reconnectPeer(Peer* peer);
    void _reconnectAll();
    bool _isQueuedCommandMapEmpty();
    bool _isNothingBlockingShutdown();
    bool _majoritySubscribed();

    // When we're a slave, we can escalate a command to the master. When we do so, we store that command in the
    // following map until the slave responds.
    map<string, SQLiteCommand> _escalatedCommandMap; // commandID -> Command map

    void _sendOutstandingTransactions();

    // How many journal tables does our DB have?
    // We always have 'journal', and then we have numbered tables 'journal00' through this number, inclusive.
    static int _maximumJournalTable;

    // The server object to which we'll pass incoming escalated commands.
    SQLiteServer& _server;
};
