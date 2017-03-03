#pragma once
#include "SQLite.h"
#include "SQLiteServer.h"

class SQLiteCommand;

extern const char* SQLCConsistencyLevelNames[];

// Distributed, master/slave, failover, transactional DB cluster
class SQLiteNode : public STCPNode {
  public: // External API

    // Possible states of a node in a DB cluster
    enum State {
        SEARCHING,     // Searching for peers
        SYNCHRONIZING, // Synchronizing with highest priority peer
        WAITING,       // Waiting for an opportunity to master or slave
        STANDINGUP,    // Taking over master-ship
        MASTERING,     // Acting as master node
        STANDINGDOWN,  // Giving up master role
        SUBSCRIBING,   // Preparing to slave to the master
        SLAVING,        // Slaving to the master node
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

    // Constructor
    SQLiteNode(SQLiteServer& server, SQLite& db, const string& name, const string& host, const string& peerList,
               int priority, uint64_t firstTimeout, const string& version, int quorumCheckpoint = 0);
    virtual ~SQLiteNode();


    // Simple accessors
    State getState() { return _state; }
    int getPriority() { return _priority; }
    string getHash() { return _db.getCommittedHash(); }
    uint64_t getCommitCount() { return _db.getCommitCount(); }
    #if 0
    list<string> getQueuedCommandList();
    list<string> getEscalatedCommandList();
    #endif
    // Returns the entire list of processed commands that are ready to be responded to.
    // Maybe this should return by reference?

    const string& getMasterVersion() { return _masterVersion; };
    const string& getVersion() { return _version; };

    // Only let a read only node have an external party set the state master version. These need to be known
    // by the read only threads even though they are replication concepts.  They let the slaves know if they
    // should accept commands, and if their version matches master so they can peek (otherwise skip peek and
    // escalate to master).
    #if 0
    void setState(State state) {
        SASSERT(_worker);
        _setState(state);
    }
    void setMasterVersion(const string& version) {
        SASSERT(_worker);
        _masterVersion = version;
    }
    #endif

    // Can shut down without causing problems
    void beginShutdown();
    bool gracefulShutdown() { return (_gracefulShutdownTimeout.alarmDuration != 0); }
    bool shutdownComplete();

    // Performs a read-only operation on the database.  Can happen in mid-
    // transaction, even if that transaction is later rolled back.
    #if 0
    string read(const string& query) { return _db.read(query); }
    bool read(const string& query, SQResult& result) { return _db.read(query, result); }
    #endif

    // Returns true when we're ready to process commands
    bool ready() { return (_state == MASTERING || _state == SLAVING); }

    #if 0
    SQLiteCommand* createCommand(const SData& request);
    SQLiteCommand* openCommand(const SData& request);
    SQLiteCommand* reopenCommand(SQLiteCommand* existingCommand);

    // Gets a completed command from the database
    SQLiteCommand* getProcessedCommand();

    // Searches through commands in all states to see if we can find one that matches
    SQLiteCommand* findCommand(const string& requestHeaderName, const string& requestHeaderValue);

    // Gets the next command queued for processsing
    // **FIXME: This is only used in BedrockServer, and the priority is a needless optimizatoin -- remove priority
    SQLiteCommand* getQueuedCommand(int priority);

    // Clears any command holds in place
    void clearCommandHolds(const string& heldBy);

    // Aborts (if active) a command on the database and cleans it up.
    // If the command has been passed to the control of a different SQLiteNode, the caller can elect not to delete it.
    void closeCommand(SQLiteCommand* command);
    #endif

    // Updates the internal state machine; returns true if it wants immediate re-updating.
    bool update(uint64_t& nextActivity);

    // Begins the process of committing a command.
    void startCommit(SQLiteCommand& command);

    // Similar to the above version, but with no explicit command. This can be used when the database has changed in a
    // way that needs to be replicated, but doesn't need to be sent back to anyone when completed. For example, a cron
    // job could update the database to contain the current free space on disk, but not need to hear the result.
    void startCommit();

    // True from when we call 'startCommit' until the commit has been sent to (and, if it required replication,
    // acknowledged by) peers.
    bool commitInProgress() { return _commitInProgress; }

    // Returns true if the last commit was successful. If called while `commitInProgress` would return true, it returns
    // the success of the last *completed* commit. If called before `startCommit` is ever called, the return value is
    // unspecified.
    bool commitSucceeded() { return _commitSucceeded; }

    // This takes a completed command and prepares to send it back to the originating peer. If this command doesn't
    // have an `originator`, then it's an error to pass it to this function.
    void queueResponse(SQLiteCommand&& command);

    // This is a utility function that we call when we finish processing a command to hand it back to the caller that
    // gave it to us in the first place. If `complete` is true, the corresponding field in the command object will also
    // be set to true to alert the caller that the command is finished and can be responded to.
    void _unblockFrontCommand(bool complete);

    // Externally exposed version of _processCommand().
    #if 0
    bool processCommand(SQLiteCommand* command);
    bool commit();
    #endif

    static atomic<bool> unsentTransactions;

    // These commands are all completed and ready for responses.
    list<SQLiteCommand> processedCommandList;

  protected:
    // STCPNode API: Peer handling framework functions
    virtual void _onConnect(Peer* peer);
    virtual void _onDisconnect(Peer* peer);
    virtual void _onMESSAGE(Peer* peer, const SData& message);
    #if 0
    // Should return true if an external queue accepted the command. If so, this node now keeps no reference to the
    // command, and the external queue is responsible for calling `reopenCommand` to return the command to this node.
    virtual bool _passToExternalQueue(SQLiteCommand* command) { return false; };

    // Wrappers for peek and process command to keep track of processing time.
    bool _peekCommandWrapper(SQLite& db, SQLiteCommand* command);
    bool _processCommandWrapper(SQLite& db, SQLiteCommand* command);
    #endif

    // Force quorum among the replica after every N commits.  This prevents master from running ahead
    // too far. "Too far" is an arbitrary threshold that trades potential loss of consistency in the
    // failure case for better performance.
    void setQuroumCheckpoint(const int quroumCheckpoint) { _quorumCheckpoint = quroumCheckpoint; };
    int getQuorumCheckpoint() { return _quorumCheckpoint; };

    //bool _worker;
    SQLite& _db;
    //map<int, list<SQLiteCommand*>> _queuedCommandMap; // priority -> list<Command*> map

    // The peer we should sync from is recalculated every time we call this. If no other peer is logged in, or no
    // logged in peer has a higher commitCount that we do, this will return null.
    void _updateSyncPeer();
    Peer* _syncPeer;

    // This lets child classes perform extra actions when our state changes
    virtual void _setState(State state) {
        _state = state;
    }

    // Synchronization variables.
    static uint64_t _lastSentTransactionID;

  private: // Internal API
  
    // Attributes
    // Escalated commands are a map for faster lookup times.  In circumstances
    // where we had many (~20k) escalated commands as a list, the slave CPUs
    // began to peg 100% as they iterated over the list
    SQLiteCommand* _currentCommand;
    string _currentTransactionCommand;
    int _priority;
    State _state;
    Peer* _masterPeer;
    uint64_t _stateTimeout;
    static atomic<int> _commandCount;
    SStopwatch _gracefulShutdownTimeout;
    string _version;
    int _quorumCheckpoint; // Commits before requiring quorum.
    int _commitsSinceCheckpoint;
    string _masterVersion;

    // This will be set by either of the `startCommit` functions.
    bool _commitInProgress;
    bool _commitSucceeded;


    // Helper methods
    void _sendToPeer(Peer* peer, const SData& message);
    void _sendToAllPeers(const SData& message, bool subscribedOnly = false);
    void _changeState(State newState);
    void _queueSynchronize(Peer* peer, SData& response, bool sendAll);
    void _recvSynchronize(Peer* peer, const SData& message);
    // void _queueCommand(SQLiteCommand* command);
    // SQLiteCommand* _finishCommand(SQLiteCommand* command);
    void _reconnectPeer(Peer* peer);
    void _reconnectAll();
    list<SQLiteCommand*> _getOrderedCommandListFromMap(const map<string, SQLiteCommand*> commandMap);
    bool _isQueuedCommandMapEmpty();
    bool _isNothingBlockingShutdown();
    bool _majoritySubscribed() {
        int ignore;
        return _majoritySubscribed(ignore, ignore);
    }
    bool _majoritySubscribed(int& numFullPeersOut, int& numFullSlavesOut);

    // When we're a slave, we can escalate a command to the master. When we do so, we store that command in the
    // following map until the slave responds.
    void _escalateCommand(SQLiteCommand&& command);
    map<string, SQLiteCommand> _escalatedCommandMap; // commandID -> Command map

    void _sendOutstandingTransactions();

    // How many journal tables does our DB have?
    // We always have 'journal', and then we have numbered tables 'journal00' through this number, inclusive.
    static int _maximumJournalTable;

    // Common functionality to `openCommand` and `reopenCommand`.
    SQLiteCommand* _finishOpeningCommand(SQLiteCommand* command);

    // Measure how much time we spend in `process()` and `COMMIT` as a fraction of total time spent.
    // Hopefully, we spend a lot of time in `process()` and relatively little in `COMMIT`, which would give us a good
    // chance of paralleling `process()` without having to figure out the same for `COMMIT`, which we don't have a
    // great solution for at the moment.
    SPerformanceTimer _processTimer;
    SPerformanceTimer _commitTimer;

    static const int MAX_PROCESS_TRIES = 3;

    // The server object to which we'll pass incoming escalated commands.
    SQLiteServer& _server;
};
