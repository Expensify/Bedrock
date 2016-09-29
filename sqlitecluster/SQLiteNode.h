// SQLiteNode.h
#pragma once
#include "SQLite.h"

// Possible states of a node in a DB cluster
enum SQLCState {
    SQLC_SEARCHING,     // Searching for peers
    SQLC_SYNCHRONIZING, // Synchronizing with highest priority peer
    SQLC_WAITING,       // Waiting for an opportnity to master or slave
    SQLC_STANDINGUP,    // Taking over mastership
    SQLC_MASTERING,     // Acting as master node
    SQLC_STANDINGDOWN,  // Giving up master role
    SQLC_SUBSCRIBING,   // Preparing to slave to the master
    SQLC_SLAVING        // Slaving to the master node
};
extern const char* SQLCStateNames[];

// Write consistencies available
enum SQLCConsistencyLevel {
    SQLC_ASYNC,  // Fully asynchronous write, no slave approval required.
    SQLC_ONE,    // Require exactly one approval (likely from a peer on the same LAN)
    SQLC_QUORUM, // Require majority approval
    SQLC_NUM_CONSISTENCY_LEVELS
};
extern const char* SQLCConsistencyLevelNames[];

// Distributed, master/slave, failover, transactional DB cluster
class SQLiteNode : public STCPNode {
  public: // External API
    // Captures all data associated with an atomic command
    struct Command {
        // Attributes
        Peer* initiator;
        string id;
        SData transaction;  // Used inside SQLiteNode
        SData request;      // Original request
        STable jsonContent; // Accumulated response content
        SData response;     // Final response
        int priority;
        uint64_t creationTimestamp;
        uint64_t replicationStartTimestamp;
        uint64_t processingTime;
        SQLCConsistencyLevel writeConsistency;

        // **NOTE: httpsRequest is used to store a pointer to a
        //         secondary SHTTPSManager request; this can be
        //         initiated in _peekCommand(), and the command won't
        //         be processed by _processCommand() until the request
        //         has completed.
        SHTTPSManager::Transaction* httpsRequest;

        // Constructor / Destructor
        Command() {
            // Initialize
            initiator = 0;
            priority = 0;
            httpsRequest = 0;
            creationTimestamp = STimeNow();
            replicationStartTimestamp = 0;
            processingTime = 0;
            writeConsistency = SQLC_ONE;
        }
        virtual ~Command() {
            // Verify clean shutdown
            SASSERTWARN(!httpsRequest);
        }
    };

    // Used to sort the escalated command map back into an ordered list
    struct Command_ptr_cmp {
        bool operator()(Command* lhs, Command* rhs) {
            return lhs->priority > rhs->priority ||
                   (lhs->priority == rhs->priority && lhs->creationTimestamp < rhs->creationTimestamp);
        }
    };
    typedef map<string, Command*>::iterator CommandMapIt;

    // Constructor
    SQLiteNode(const string& filename, const string& name, const string& host, int priority, int cacheSize,
               int autoCheckpoint, uint64_t firstTimeout, const string& version, int quorumCheckpoint = 0,
               const string& synchronousCommands = "", bool readOnly = false, int maxJournalSize = 1000000);
    virtual ~SQLiteNode();

    // Simple accessors
    SQLCState getState() { return _state; }
    int getPriority() { return _priority; }
    string getHash() { return _db.getCommittedHash(); }
    uint64_t getCommitCount() { return _db.getCommitCount(); }
    list<string> getQueuedCommandList();
    list<string> getEscalatedCommandList();
    list<string> getProcessedCommandList();
    const string& getMasterVersion() { return _masterVersion; };
    const string& getVersion() { return _version; };

    // Only let a read only node have an external party set the state master version. These need to be known
    // by the read only threads even though they are replication concepts.  They let the slaves know if they
    // should accept commands, and if their version matches master so they can peek (otherwise skip peek and
    // escalate to master).
    void setState(SQLCState state) {
        SASSERT(_readOnly);
        _state = state;
    }
    void setMasterVersion(const string& version) {
        SASSERT(_readOnly);
        _masterVersion = version;
    }

    // Can shut down without causing problems
    void beginShutdown();
    bool gracefulShutdown() { return (_gracefulShutdownTimeout.alarmDuration != 0); }
    bool shutdownComplete();

    // Performs a read-only operation on the database.  Can happen in mid-
    // transaction, even if that transaction is later rolled back.
    string read(const string& query) { return _db.read(query); }
    bool read(const string& query, SQResult& result) { return _db.read(query, result); }

    // Returns true when we're ready to process commands
    bool ready() { return (_state == SQLC_MASTERING || _state == SQLC_SLAVING); }

// Executes a new command on the distributed database with a given priority.
// High priority commands take precedence over low, otherwise it's FIFO.
#define SPRIORITY_MAX 1000
#define SPRIORITY_HIGH 750
#define SPRIORITY_NORMAL 500
#define SPRIORITY_LOW 250
#define SPRIORITY_MIN 0
    Command* openCommand(const SData& request, int priority, bool unique = false, int64_t commandExecuteTime = 0);

    // Gets a completed command from the database
    Command* getProcessedCommand();

    // Searches through commands in all states to see if we can find one that matches
    Command* findCommand(const string& requestHeaderName, const string& requestHeaderValue);

    // Gets the next command queued for processsing
    // **FIXME: This is only used in BedrockServer, and the priority is a needless optimizatoin -- remove priority
    Command* getQueuedCommand(int priority);

    // Clears any command holds in place
    void clearCommandHolds(const string& heldBy);

    // Aborts (if active) a command on the database and cleans it up.
    void closeCommand(Command* command);

    // Updates the internal state machine; returns true if it wants immediate
    // re-updating.
    bool update(uint64_t& nextActivity);

    // STCPNode API: Peer handling framework functions
    virtual void _onConnect(Peer* peer);
    virtual void _onDisconnect(Peer* peer);
    virtual void _onMESSAGE(Peer* peer, const SData& message);

    // Parent overrides these in order to process commands.  Return true in
    // _processCommand() or _peekCommand to signal that the command is complete
    // and ready to commit (assuming any uncommitted transaction has been
    // started). _abortCommand() is called if we cannot complete this command.
    //
    // _processCommand() is called without db having an active transaction --
    // if you want to start a transaction, you're free to do so.  But you must
    // prepare any non-empty uncommitted transaction before returning.  Also,
    // you must rollback any empty (or aborted) uncommitted transaction before
    // returning.
    //
    // Use _peekCommand() if you want to perform any pre-processing on the
    // command before queueing.  This might include triggering a background
    // operation to get a head-start on the command, or even doing the entire
    // command without waiting.  (This would only work for
    // read-only/non-mutable commands; anything changing the database would
    // need to be queued so we do it in order.)  Return true to mark the
    // command as completed without ever actually queueing it.  (If you do
    // start a background operation, however, be sure executing that operation
    // twice is non-damaging as it can be repeated in some edge cases involving
    // server failure.)
    //
    // _cleanCommand() is called when the command is closed; use it for any
    // final cleanup operations.
    //
    virtual bool _peekCommand(SQLite& db, Command* command) = 0;
    virtual void _processCommand(SQLite& db, Command* command) = 0;
    virtual void _abortCommand(SQLite& db, Command* command) = 0;
    virtual void _cleanCommand(Command* command) = 0;

    // Wrappers for peek and process command to keep track of processing time.
    bool _peekCommandWrapper(SQLite& db, Command* command) {
        // Measure elapsed time and add to processing
        uint64_t start = STimeNow();
        bool result = _peekCommand(db, command);
        command->processingTime += STimeNow() - start;
        return result;
    }
    void _processCommandWrapper(SQLite& db, Command* command) {
        // Measure elapsed time and add to processing
        uint64_t start = STimeNow();
        _processCommand(db, command);
        command->processingTime += STimeNow() - start;
    }

    // Force quorum among the replica after every N commits.  This prevents master from running ahead
    // too far. "Too far" is an arbitrary threshold that trades potential loss of consistency in the
    // failure case for better performance.
    void setQuroumCheckpoint(const int quroumCheckpoint) { _quorumCheckpoint = quroumCheckpoint; };
    int getQuorumCheckpoint() { return _quorumCheckpoint; };

  protected:
    bool _readOnly;
    SQLite _db;
    map<int, list<Command*>> _queuedCommandMap; // priority  -> list<Command*> map

  private: // Internal API
    // Attributes
    // Escalated commands are a map for faster lookup times.  In circumstances
    // where we had many (~20k) escalated commands as a list, the slave CPUs
    // began to peg 100% as they iterated over the list
    Command* _currentCommand;
    string _currentTransactionCommand;
    int _priority;
    SQLCState _state;
    map<string, Command*> _escalatedCommandMap; // commandID -> Command* map
    list<Command*> _processedCommandList;
    Peer* _syncPeer;
    Peer* _masterPeer;
    uint64_t _stateTimeout;
    int _commandCount;
    SStopwatch _gracefulShutdownTimeout;
    string _version;
    int _quorumCheckpoint; // Commits before requiring quorum.
    int _commitsSinceCheckpoint;
    list<string> _synchronousCommands;
    string _masterVersion;

    // Helper methods
    void _sendToPeer(Peer* peer, const SData& message);
    void _sendToAllPeers(const SData& message, bool subscribedOnly = false);
    void _changeState(SQLCState newState);
    void _queueSynchronize(Peer* peer, SData& response, bool sendAll);
    void _recvSynchronize(Peer* peer, const SData& message);
    void _queueCommand(Command* command);
    void _escalateCommand(Command* command);
    void _finishCommand(Command* command);
    void _reconnectPeer(Peer* peer);
    void _reconnectAll();
    list<Command*> _getOrderedCommandListFromMap(const map<string, Command*> commandMap);
    bool _isQueuedCommandMapEmpty();
    bool _isNothingBlockingShutdown();
    bool _majoritySubscribed() {
        int ignore;
        return _majoritySubscribed(ignore, ignore);
    }
    bool _majoritySubscribed(int& numFullPeersOut, int& numFullSlavesOut);
};
