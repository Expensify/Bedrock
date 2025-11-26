#pragma once
#include <libstuff/libstuff.h>
#include <libstuff/SSynchronizedQueue.h>
#include <libstuff/STCPManager.h>
#include <sqlitecluster/SQLite.h>
#include <sqlitecluster/SQLitePool.h>

#include <mutex>
#include <condition_variable>
#include <queue>
#include <thread>

// This file is long and complex. For each nested sub-structure (I.e., classes inside classes) we have attempted to
// arrange things as such:
// For each public/private block:
// 1. classes/structs/type definitions
// 2. static members
// 3. static methods
// 4. const methods
// 5. non-const methods
// 6. const instance members
// 7. non-const instance members.
// 8. In each of these sections, things should be alphabetized.

/*
 * Rules for maintaining SQLiteNode methods so that atomicity works as intended.
 *
 * No non-const members should be publicly exposed.
 * Any public method that is `const` must shared_lock<>(_stateMutex).
 * Alternatively, a public `const` method that is a simple getter for an atomic property can skip the lock.
 * Any public method that is non-const must unique_lock<>(_stateMutex) before changing any internal state, and must hold
 * this lock until it is done changing state to make this method's changes atomic.
 * Any private methods must not call public methods.
 * Any private methods must not lock _stateMutex (for recursion reasons).
 * Any public methods must not call other public methods.
 *
 * `_replicate` is a special exception because it runs in multiple threads internally. It needs to handle locking if it
 * changes any internal state (and it calls `changeState`, which does).
 *
 */

class SQLiteCommand;
class SQLiteServer;
class SQLitePeer;

// Possible states of a node in a DB cluster
enum class SQLiteNodeState {
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

// Distributed, leader/follower, failover, transactional DB cluster
class SQLiteNode : public STCPManager {
    // This exists to expose internal state to a test harness. It is not used otherwise.
    friend struct SQLiteNodeTest;
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

    // This is a globally accessible pointer to some node instance. The intention here is to let signal handling code attempt to kill outstanding
    // peer connections on this node before shutting down. Generally, there is only a single node instance per application, and this will
    // refer to that node, there may be exceptions in cases like test harnesses.
    static SQLiteNode* KILLABLE_SQLITE_NODE;

    // If the above node has been killed, this will be set. The intention is to be able to detect and avoid various race conditions,
    // i.e., immediately reconnecting a network socket after killing it.
    static atomic<bool> NODE_KILLED;

    // Receive timeout for cluster messages.
    static const uint64_t RECV_TIMEOUT;

    // The minimum frequency of APPROVE_TRANSACTION messages we'll send when following, back to leader, to indicate our own current synchronization state.
    // This is expressed as "every Nth message", where e.g., if MIN_APPROVE_FREQUENCY is 10, we will respond to at least every 10th BEGIN_TRANSACTION message.
    static const size_t MIN_APPROVE_FREQUENCY;

    // Get and SQLiteNode State from it's name.
    static SQLiteNodeState stateFromName(const string& name);

    // Return the string representing an SQLiteNode State
    static const string& stateName(SQLiteNodeState state);

    // True from when we call 'startCommit' until the commit has been sent to (and, if it required replication,
    // acknowledged by) peers.
    // Does not block.
    bool commitInProgress() const;

    // Returns true if the last commit was successful. If called while `commitInProgress` would return true, it returns false.
    // Does not block.
    bool commitSucceeded() const;

    // Get's the commitCount from the underlying DB.
    // Does not block.
    uint64_t getCommitCount() const;

    // Get's the number of WAL frames that are currently waiting to be checkpointed.
    uint64_t getOutstandingFramesToCheckpoint() const;

    // Get's the current leader version (our own version if we're leading)
    // Can block.
    const string getLeaderVersion() const;

    // Gets a copy of the peer state as an STable.
    // Can block.
    list<STable> getPeerInfo() const;

    // Gets a random follower peer that is in the same version as leader.
    string getEligibleFollowerForForwardingAddress() const;

    // Returns our current priority.
    // Does not block.
    int getPriority() const;

    // Sets the node priority to 1 and broadcasts STATE to the cluster.
    // Can block.
    void setShutdownPriority();

    // Returns our current state.
    // Does not block.
    SQLiteNodeState getState() const;

    // Returns true if we're LEADING with enough FOLLOWERs to commit a quorum transaction.
    // Can block.
    bool hasQuorum() const;

    // Return the command address of the current leader, if there is one (empty string otherwise).
    // Can block.
    string leaderCommandAddress() const;

    // Return the state of the lead peer. Returns UNKNOWN if there is no leader, or if we are the leader.
    // Does not block.
    SQLiteNodeState leaderState() const;

    // Tell the node a commit has been made by another thread, so that we can interrupt our poll loop if we're waiting
    // for data, and send the new commit.
    // Does not block.
    void notifyCommit() const;

    // Prepare a set of sockets to wait for read/write.
    // Can block.
    void prePoll(fd_map& fdm) const;

    // Call this to check if the node's completed shutting down.
    // Can block.
    bool shutdownComplete() const;

    // Call this if you want to shut down the node. Returns true if shutdown was initiated,
    // false if shutdown was already happening.
    bool beginShutdown();

    // kill all peer connections on this node.
    void kill();

    // Handle any read/write events that occurred.
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Constructor/Destructor
    SQLiteNode(SQLiteServer& server, const shared_ptr<SQLitePool>& dbPool, const string& name, const string& host,
               const string& peerList, int priority, uint64_t firstTimeout, const string& version,
               const string& commandPort = "localhost:8890");
    ~SQLiteNode();

    // Begins the process of committing a transaction on this SQLiteNode's database. When this returns,
    // commitInProgress() will return true until the commit completes.
    void startCommit(ConsistencyLevel consistency);

    // Updates the internal state machine. Returns true if it wants immediate re-updating. Returns false to indicate it
    // would be a good idea for the caller to read any new commands or traffic from the network.
    bool update();

    // Look up the correct peer by the name it supplies in a LOGIN
    // message. Does not lock, but this method is const and all it does is
    // access _peerList and peer->name, both of which are const. So it is safe
    // to call from other public functions.
    SQLitePeer* getPeerByName(const string& name) const;

    // Pointer to the current on prepare handler to be passed on commit to SQLite
    void (*onPrepareHandler)(SQLite& _db, int64_t tableID);
    bool onPrepareHandlerEnabled;

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

    static const vector<SQLitePeer*> _initPeers(const string& peerList);

    // Queue a SYNCHRONIZE message based on the current state of the node, thread-safe, but you need to pass the
    // *correct* DB for the thread that's making the call (i.e., you can't use the node's internal DB from a worker
    // thread with a different DB object) - which is why this is static.
    static void _queueSynchronize(const SQLiteNode* const node, SQLitePeer* peer, SQLite& db, SData& response, bool sendAll, uint64_t timeoutAfterUS = 0);

    bool _isNothingBlockingShutdown() const;
    bool _majoritySubscribed() const;

    Socket* _acceptSocket();

    // Add required headers for messages being sent to peers.
    SData _addPeerHeaders(SData message);

    void _changeState(SQLiteNodeState newState, uint64_t commitIDToCancelAfter = 0);

    string _getLostQuorumLogMessage() const;

    // Handlers for transaction messages.
    void _handleBeginTransaction(SQLite& db, SQLitePeer* peer, const SData& message);
    void _handlePrepareTransaction(SQLite& db, SQLitePeer* peer, const SData& message, uint64_t dequeueTime);
    int _handleCommitTransaction(SQLite& db, SQLitePeer* peer, const uint64_t commandCommitCount, const string& commandCommitHash);
    void _handleRollbackTransaction(SQLite& db, SQLitePeer* peer, const SData& message);

    // Called when we first establish a connection with a new peer
    void _onConnect(SQLitePeer* peer);

    // Called when we lose connection with a peer
    void _onDisconnect(SQLitePeer* peer);

    // Called when the peer sends us a message; throw an SException to reconnect.
    void _onMESSAGE(SQLitePeer* peer, const SData& message);
    void _reconnectAll();
    void _reconnectPeer(SQLitePeer* peer);
    void _recvSynchronize(SQLitePeer* peer, const SData& message);

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
    void _replicate();

    // Replicates any transactions that have been made on our database by other threads to peers.
    void _sendOutstandingTransactions(const set<uint64_t>& commitOnlyIDs = {});
    void _sendStandupResponse(SQLitePeer* peer, const SData& message);
    void _sendPING(SQLitePeer* peer);
    void _sendToAllPeers(const SData& message, bool subscribedOnly = false);
    void _sendToPeer(SQLitePeer* peer, const SData& message);

    // Choose the best peer to synchronize from. If no other peer is logged in, or no logged in peer has a higher
    // commitCount that we do, this will return null.
    void _updateSyncPeer();

    void _dieIfForkedFromCluster();

    void _processPeerMessages(uint64_t& nextActivity, SQLitePeer* peer, bool unlimited = false);

    const string _commandAddress;
    const string _name;
    const string _host;
    const vector<SQLitePeer*> _peerList;

    // When the node starts, it is not ready to serve requests without first connecting to the other nodes, and checking
    // to make sure it's up-to-date. Store the configured priority here and use "-1" until we're ready to fully join the cluster.
    const int _originalPriority;

    // Tracks whether this node has seen at least one peer running the same version as itself.
    // We use this along with _haveBeenWAITING to determine when to restore the node's original priority after startup.
    // This prevents a node started on the wrong (perhaps out-of-date) version from taking over leadership and thus having
    // the entirety of traffic to the cluster directed to it.
    bool _haveSeenPeerOnSameVersion = false;

    // Tracks whether this node has ever entered the WAITING state during its lifecycle.
    // We will avoid having a high priority until after synchronization is complete becuase if we are the expected leader,
    // with the highest priority, the secondary leader will stand down when it sees us online. However, if synchronization
    // is not complete, this can leave the cluster with no leader for a long time. We will never go WAITING until we've run
    // through synchronization once, so this avoids that scenario.
    bool _haveBeenWAITING = false;

    // A string representing an address (i.e., `127.0.0.1:80`) where this server accepts commands. I.e., "the command port".
    unique_ptr<Port> _port;

    // Our version string. Supplied by constructor.
    const string _version;

    // These are sockets that have been accepted on the node port but have not yet been associated with a peer (because
    // they need to send a LOGIN message with their name first).
    set<Socket*> _unauthenticatedIncomingSockets;

    // The write consistency requested for the current in-progress commit.
    // Remove. See: https://github.com/Expensify/Expensify/issues/208443
    ConsistencyLevel _commitConsistency;

    // This is the current CommitState we're in with regard to committing a transaction. It is `UNINITIALIZED` from
    // startup until a transaction is started.
    atomic<CommitState> _commitState;

    // This is just here to allow `poll` to get interrupted when there are new commits to send. We don't want followers
    // to wait up to a full second for them.
    mutable SSynchronizedQueue<bool> _commitsToSend;

    // Handle to the underlying database that we write to. This should also be passed to an SQLiteCore object that can
    // actually perform some action on the DB. When those action are complete, you can call SQLiteNode::startCommit()
    // to commit and replicate them.
    SQLite& _db;

    // This is a pool of DB handles that this node can use for any DB access it needs. Currently, it hands them out to
    // replication threads as required. It's passed in via the constructor.
    shared_ptr<SQLitePool> _dbPool;

    // Set to true to indicate we're attempting to shut down.
    atomic<bool> _isShuttingDown;

    // When we spontaneously lose quorum (due to an unexpected node disconnection) we log the time. Later, if we detect we've forked,
    // We show this time in a log line as a diagnostic message.
    atomic<uint64_t> _lastLostQuorum = 0;

    // Store the ID of the last transaction that we replicated to peers. Whenever we do an update, we will try and send
    // any new committed transactions to peers, and update this value.
    uint64_t _lastSentTransactionID;

    // Pointer to the peer that is the leader. Null if we're the leader, or if we don't have a leader yet.
    atomic<SQLitePeer*> _leadPeer;

    // We can spin up threads to handle responding to `SYNCHRONIZE` messages out-of-band. We want to make sure we don't
    // shut down in the middle of running these, so we keep a count of them.
    atomic<size_t> _pendingSynchronizeResponses = 0;

    // Our priority, with respect to other nodes in the cluster. This is passed in to our constructor. The node with
    // the highest priority in the cluster will attempt to become the leader.
    // This is the same as `_originalPriority` most of the time except when we're first starting up and synchronizing,
    // or when we're standingdown.
    // Remove. See: https://github.com/Expensify/Expensify/issues/208449
    atomic<int> _priority;

    // Server that implements `SQLiteServer` interface.
    SQLiteServer& _server;

    // Stopwatch to track if we're giving up on the server preventing a standdown.
    SStopwatch _standDownTimeout;

    // Our current State.
    atomic<SQLiteNodeState> _state;

    // Keeps track if we have closed the command port for commits fallen behind.
    bool _blockedCommandPortForBeingBehind{false};

    // This is an integer that increments every time we change states. This is useful for responses to state changes
    // (i.e., approving standup) to verify that the messages we're receiving are relevant to the current state change,
    // and not stale responses to old changes.
    int _stateChangeCount;

    // This is the mutex we lock any time we change any internal state variables.
    mutable shared_mutex _stateMutex;

    // Timestamp that, if we pass with no activity, we'll give up on our current state, and start over from SEARCHING.
    uint64_t _stateTimeout;

    // The peer that we'll synchronize from.
    // Remove. See: https://github.com/Expensify/Expensify/issues/208439
    SQLitePeer* _syncPeer;

    // A pointer to a SQLite instance that is passed to plugin's stateChanged function. This prevents plugins from operating on the same handle that
    // the sync node is when they run queries in stateChanged.
    SQLite* pluginDB;

    thread* _replicateThread = nullptr;
    mutex _replicateMutex;
    condition_variable _replicateCV;
    queue<pair<SQLitePeer*, SData>> _replicateQueue;
    atomic<uint64_t> _replicateThreadShouldExitTime;
};
