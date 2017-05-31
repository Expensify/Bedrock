#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLiteServer.h>
#include "BedrockPlugin.h"
#include "BedrockCommandQueue.h"

class BedrockServer : public SQLiteServer {
  public:
    // This is the list of plugins that we're actually using, which is a subset of all available plugins. It will be
    // initialized at construction based on the arguments passed in.
    list<BedrockPlugin*> plugins;

    // A command queue is just a SSynchronizedQueue of BedrockCommands. This is distinct from a `BedrockCommandQueue`,
    // which is a more complex data structure.
    typedef SSynchronizedQueue<BedrockCommand> CommandQueue;

    // Our only constructor.
    BedrockServer(const SData& args);

    // Destructor
    virtual ~BedrockServer();

    // Accept an incoming command from an SQLiteNode.
    // SQLiteNode API.
    void acceptCommand(SQLiteCommand&& command);

    // Cancel a command.
    // SQLiteNode API.
    void cancelCommand(const string& commandID);

    // Returns true when everything's ready to shutdown.
    bool shutdownComplete();

    // Exposes the replication state to plugins.
    SQLiteNode::State getState() const { return _replicationState.load(); }

    // Flush the send buffers
    // STCPNode API.
    void prePoll(fd_map& fdm);

    // Accept connections and dispatch requests
    // STCPNode API.
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Control the command port. The server will toggle this as necessary, unless manualOverride is set,
    // in which case the `suppress` setting will be forced.
    void suppressCommandPort(bool suppress, bool manualOverride = false);

    // This will return true if there's no outstanding writable activity that we're waiting on. It's called by an
    // SQLiteNode in a STANDINGDOWN state to know that it can switch to searching.
    virtual bool canStandDown();

  private:
    // The name of the sync thread.
    static constexpr auto _syncThreadName = "sync";

    // Arguments passed on the command line. This is modified internally and used as a general attribute store.
    SData _args;

    // Commands that aren't currently being processed are kept here.
    BedrockCommandQueue _commandQueue;

    // Each time we read a new request from a client, we give it a unique ID.
    uint64_t _requestCount;

    // We keep a map of requests to socket. We should never have more than one request per socket at a given time, or
    // we could deliver responses in the wrong order.
    map<uint64_t, Socket*> _requestCountSocketMap;

    // Each time we connect a new socket, we give it an ID, and we insert it in this set. When a socket disconnects, we
    // remove that ID from this set.
    map <uint64_t, Socket*> _socketIDMap;

    // The above _socketIDMap is modified by multiple threads, so we lock this mutex around operations that modify it.
    recursive_mutex _socketIDMutex;

    // This is the replication state of the sync node. It's updated after every SQLiteNode::update() iteration. A
    // reference to this object is passed to the sync thread to allow this update.
    atomic<SQLiteNode::State> _replicationState;

    // This gets set to true when a database upgrade is in progress, letting workers know not to try to start any work.
    atomic<bool> _upgradeInProgress;

    // This flag will be raised when we want to start shutting down. A reference is passed to the sync thread to allow
    // it to shut down its SQLiteNode.
    atomic<bool> _nodeGracefulShutdown;

    // This is the current version of the master node, updated after every SQLiteNode::update() iteration. A
    // reference to this object is passed to the sync thread to allow this update.
    atomic<string> _masterVersion;

    // This is a synchronized queued that can wake up a `poll()` call if something is added to it. This contains the
    // list of commands that worker threads were unable to complete on their own that needed to be passed back to the
    // sync thread. A reference is passed to the sync thread.
    CommandQueue _syncNodeQueuedCommands;

    // These control whether or not the command port is currently opened.
    bool _suppressCommandPort;
    bool _suppressCommandPortManualOverride;

    // This is a map of open listening ports to the plugin objects that created them.
    map<Port*, BedrockPlugin*> _portPluginMap;

    // The server version. This may be fake if the arguments contain a `versionOverride` value.
    string _version;

    // The actual thread object for the sync thread.
    thread _syncThread;

    // Give all of our plugins a chance to verify and/or modify the database schema. This will run every time this node
    // becomes master. It will return true if the DB has changed and needs to be committed.
    bool _upgradeDB(SQLite& db);

    // Iterate across all of our plugins and call `prePoll` and `postPoll` on any httpsManagers they've created.
    void _prePollPlugins(fd_map& fdm);
    void _postPollPlugins(fd_map& fdm, uint64_t nextActivity);

    // This is the function that launches the sync thread, which will bring up the SQLiteNode for this server, and then
    // start the worker threads.
    static void sync(SData& args,
                     atomic<SQLiteNode::State>& replicationState,
                     atomic<bool>& upgradeInProgress,
                     atomic<bool>& nodeGracefulShutdown,
                     atomic<string>& masterVersion,
                     CommandQueue& syncNodeQueuedCommands,
                     BedrockServer& server);

    // Each worker thread runs this function. It gets the same data as the sync thread, plus its individual thread ID.
    static void worker(SData& args,
                       atomic<SQLiteNode::State>& _replicationState,
                       atomic<bool>& upgradeInProgress,
                       atomic<bool>& nodeGracefulShutdown,
                       atomic<string>& masterVersion,
                       CommandQueue& syncNodeQueuedCommands,
                       CommandQueue& syncNodeCompletedCommands,
                       BedrockServer& server,
                       int threadId,
                       int threadCount);

    // Send a reply for a completed command back to the initiating client. If the `originator` of the command is set,
    // then this is an error, as the command should have been sent back to a peer.
    void _reply(BedrockCommand&);

    // The following are constants used as methodlines by status command requests.
    static constexpr auto STATUS_IS_SLAVE          = "GET /status/isSlave HTTP/1.1";
    static constexpr auto STATUS_HANDLING_COMMANDS = "GET /status/handlingCommands HTTP/1.1";
    static constexpr auto STATUS_PING              = "Ping";
    static constexpr auto STATUS_STATUS            = "Status";
    static constexpr auto STATUS_WHITELIST         = "SetCommandWhitelist";

    // This *only* exists so that status commands can pull info from this node.
    SQLiteNode* _syncNode;

    // Because status will access internal sync node data, we lock in both places that will access the pointer above.
    mutex _syncMutex;

    // Functions for checking for and responding to status commands.
    bool _isStatusCommand(BedrockCommand& command);
    void _status(BedrockCommand& command);

    // This counts the number of commands that are being processed that might be able to write to the database. We
    // won't start any of these unless we're mastering, and we won't allow SQLiteNode to drop out of STANDINGDOWN until
    // it's 0.
    atomic<int> _writableCommandsInProgress;

    // This is a map of commit counts in the future to commands that depend on them. We can receive a command that
    // depends on a future commit if we're a slave that's behind master, and a client makes two requests, one to a node
    // more current than ourselves, and a following request to us. We'll move these commands to this special map until
    // we catch up, and then move them back to the regular command queue.
    multimap<uint64_t, BedrockCommand> _futureCommitCommands;
    recursive_mutex _futureCommitCommandMutex;

    // This is a shared mutex. It can be locked by many readers at once, but if the writer (the sync thread) locks it,
    // no other thread can access it. It's locked by the sync thread immediately before starting a transaction, and
    // unlocked afterward. Workers do the same, so that they won't try to start a new transaction while the sync thread
    // is committing. This mutex is *not* recursive.
    shared_timed_mutex _syncThreadCommitMutex;

    // A set of command names that will always be run with QUORUM consistency level.
    // Specified by the `-synchronousCommands` command-line switch.
    set<string> _syncCommands;

    // This is a list of command names than can be processed and committed in worker threads.
    static set<string> _parallelCommands;
    static recursive_mutex  _parallelCommandMutex;

    // Stopwatch to track if we're going to give up on gracefully shutting down and force it.
    SStopwatch _gracefulShutdownTimeout;
};
