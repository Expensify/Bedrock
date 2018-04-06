#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLiteServer.h>
#include "BedrockPlugin.h"
#include "BedrockCommandQueue.h"

class BedrockServer : public SQLiteServer {
  public:

    // There are two interesting cases for "shutting down" a BedrockServer. One is actually turning it off and the
    // other is standing it down from master to slave. These have different control flows, but are both discussed here
    // as they relate to one another.
    //
    // Let's start with actually shutting a server down. When a server comes up, it will be in a RUNNING state. This is
    // the normal operating state and everything proceeds as normal. When we ask it to shutdown (typically by sending
    // it a SIGINT), it will close it's command port, and set the shutdown state to START_SHUTDOWN.
    // At this point, every time it replies to a command, it will add `Connection: close` to the response headers, and
    // close the socket to the client after this response. Since it's no longer accepting new sockets on the command
    // port, it will eventually close all client sockets as all pending commands are responded to. If the server is
    // MASTER, it will start to reject escalated commands in this state as well, so as to eventually be able to empty
    // all of it's queues. Those commands will be re-escalated to a new master once this one finishes shutting down.
    // When `postPoll` runs after checking for network activity, if there are no client sockets left, and we are in
    // the START_SHUTDOWN state, we will increment to CLIENTS_RESPONDED.
    // The next `update` loop of the sync thread will notice we're at the CLIENTS_RESPONDED state, and it will begin
    // shutting down the sync node itself. If it's mastering, it will stand down and allow another node to stand up.
    // When the sync node finishes shutting down, it will set the state to DONE. Worker threads will notice this and
    // return, and the sync thread will join them and the entire BedrockServer at that point is complete and can be
    // destructed.
    //
    // Standing down is the other interesting case. Standing down only occurs if we were first mastering, and when
    // shutting down, happens in between the CLIENTS_RESPONDED and DONE states. Standing down when shutting down is
    // actually pretty straightforward, as we've already completed all outstanding commands and are rejecting any new
    // ones, so there are no strange edge cases to handle.
    // However, we stand down without shutting down when another, higher-priority master stands up, and we need to
    // become a slave to it. In this case, we need to finish any commands that need to be run on master, but we don't
    // need to finish *all* commands, we can process any commands that can run on a slave after we're done.
    // Which commands need to be run on master? Any command that will do `processCommand`, including HTTPS commands.
    // Also, any command that was escalated from another node to this one (because we don't do chained escalations).
    //
    // When the syncNode informs us that we're standing down (it will be the object that notices another master wants
    // to stand up), it will start rejecting escalated commands. We will also start putting new commands from local
    // clients in to a temporary "stand down queue" of commands that won't be run until after we've stood down. These
    // two changes limit us to running through the existing queue of commands to shut down. We need to run through this
    // entire queue, because many of the commands in the queue will be empty.
    // We then block standing down via the `canStandDown()` until our queues (except the "stand down queue") are empty,
    // and no commands are in progress. A command is "in progress" from when it's removed from the queue until its
    // response is sent. Commands escalated from workers to the sync thread are "in progress" until they complete.
    enum SHUTDOWN_STATE {
        RUNNING,
        START_SHUTDOWN,
        CLIENTS_RESPONDED,
        DONE
    };

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
    // `isNew` will be set to true if this command has never been seen before, and false if this is an existing command
    // being returned to the command queue.
    void acceptCommand(SQLiteCommand&& command, bool isNew);

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

    // When a peer node logs in, we'll send it our crash command list.
    void onNodeLogin(SQLiteNode::Peer* peer);

    // Accept connections and dispatch requests
    // STCPNode API.
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Control the command port. The server will toggle this as necessary, unless manualOverride is set,
    // in which case the `suppress` setting will be forced.
    void suppressCommandPort(const string& reason, bool suppress, bool manualOverride = false);

    // This will return true if there's no outstanding writable activity that we're waiting on. It's called by an
    // SQLiteNode in a STANDINGDOWN state to know that it can switch to searching.
    virtual bool canStandDown();

    // Returns whether or not this server was configured to backup when it completed shutdown.
    bool backupOnShutdown();

    // Returns a copy of the internal state of the sync node's peers. This can be empty if there are no peers, or no
    // sync node.
    list<STable> getPeerInfo();

    // Send a command to all of our peers. It will be wrapped appropriately.
    void broadcastCommand(const SData& message);

  private:
    // The name of the sync thread.
    static constexpr auto _syncThreadName = "sync";

    // Arguments passed on the command line. This is modified internally and used as a general attribute store.
    SData _args;

    // Commands that aren't currently being processed are kept here.
    BedrockCommandQueue _commandQueue;

    // Each time we read a new request from a client, we give it a unique ID.
    uint64_t _requestCount;

    // Each time we read a command off a socket, we put the socket in this map, so that we can respond to it when the
    // command completes. We remove the socket from the map when we reply to the command, even if the socket is still
    // open.
    map <uint64_t, Socket*> _socketIDMap;

    // The above _socketIDMap is modified by multiple threads, so we lock this mutex around operations that modify it.
    recursive_mutex _socketIDMutex;

    // This is the replication state of the sync node. It's updated after every SQLiteNode::update() iteration. A
    // reference to this object is passed to the sync thread to allow this update.
    atomic<SQLiteNode::State> _replicationState;

    // This gets set to true when a database upgrade is in progress, letting workers know not to try to start any work.
    atomic<bool> _upgradeInProgress;

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
                     atomic<string>& masterVersion,
                     CommandQueue& syncNodeQueuedCommands,
                     BedrockServer& server);

    // Wraps the sync thread main function to make it easy to add exception handling.
    static void syncWrapper(SData& args,
                     atomic<SQLiteNode::State>& replicationState,
                     atomic<bool>& upgradeInProgress,
                     atomic<string>& masterVersion,
                     CommandQueue& syncNodeQueuedCommands,
                     BedrockServer& server);

    // Each worker thread runs this function. It gets the same data as the sync thread, plus its individual thread ID.
    static void worker(SData& args,
                       atomic<SQLiteNode::State>& _replicationState,
                       atomic<bool>& upgradeInProgress,
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
    static constexpr auto STATUS_BLACKLIST         = "SetParallelCommandBlacklist";
    static constexpr auto STATUS_MULTIWRITE        = "EnableMultiWrite";

    // This makes the sync node available to worker threads, so that they can write to it's sockets, and query it for
    // data (such as in the Status command). Because this is a shared pointer, the underlying object can't be deleted
    // until all references to it go out of scope. Since an STCPNode never deletes `Peer` objects until it's being
    // destroyed, we are also guaranteed that all peers are accessible as long as we hold a shared pointer to this
    // object.
    shared_ptr<SQLiteNode> _syncNode;

    // Because status will access internal sync node data, we lock in both places that will access the pointer above.
    recursive_mutex _syncMutex;

    // Functions for checking for and responding to status and control commands.
    bool _isStatusCommand(BedrockCommand& command);
    void _status(BedrockCommand& command);
    bool _isControlCommand(BedrockCommand& command);
    void _control(BedrockCommand& command);

    // Accepts any sockets pending on our listening ports. We do this both after `poll()`, and before shutting down
    // those ports.
    void _acceptSockets();

    // This stars the server shutting down.
    void _beginShutdown(const string& reason, bool detach = false);

    // This counts the number of commands currently being processed (which might not be in any of our queues). We use
    // this value to prevent us from standing down until this value is 0 and our main queue is empty.
    atomic<int> _commandsInProgress;

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

    // Set this when we switch mastering.
    atomic<bool> _suppressMultiWrite;

    // A set of command names that will always be run with QUORUM consistency level.
    // Specified by the `-synchronousCommands` command-line switch.
    set<string> _syncCommands;

    // This is a list of command names than can be processed and committed in worker threads.
    static set<string> _blacklistedParallelCommands;
    static recursive_mutex  _blacklistedParallelCommandMutex;

    // Stopwatch to track if we're going to give up on gracefully shutting down and force it.
    SStopwatch _gracefulShutdownTimeout;

    // The current state of shutdown. Starts as RUNNING.
    atomic<SHUTDOWN_STATE> _shutdownState;

    // Flag indicating whether multi-write is enabled.
    atomic<bool> _multiWriteEnabled;

    // Set this to cause a backup to run when the server shuts down.
    bool _backupOnShutdown;
    bool _detach;

    // Pointer to the control port, so we know which port not to shut down when we close the command ports.
    Port* _controlPort;
    Port* _commandPort;

    // The following variables all exist to to handle commands that seem to have caused crashes. This lets us broadcast
    // a command to all peer nodes with information about the crash-causing command, so they can refuse to process it if
    // it gets sent again (i.e., if an end-user clicks 'refresh' after crashing the first node). Because these can
    // originate in worker threads, much of this is synchronization code to make sure the sync thread can send this
    // message before the worker exits.

    // A shared mutex to control access to the list of crash-inducing commands.
    shared_timed_mutex _crashCommandMutex;

    // Definitions of crash-causing commands. This is a map of methodLine to name/value pairs required to match a
    // particular command for it count as a match likely to cause a crash.
    map<string, set<STable>> _crashCommands;

    // Returns whether or not the command was a status or control command. If it was, it will have already been handled
    // and responded to upon return
    bool _handleIfStatusOrControlCommand(BedrockCommand& command);

    // Check a command against the list of crash commands, and return whether we think the command would crash.
    bool _wouldCrash(const BedrockCommand& command);

    // Generate a CRASH_COMMAND command for a given bad command.
    static SData _generateCrashMessage(const BedrockCommand* command);

    // This is a map of HTTPS requests to the commands that contain them. We use this to quickly look up commands when
    // their https requests finish and move them back to the main queue.
    mutex _httpsCommandMutex;
    map<SHTTPSManager::Transaction*, BedrockCommand> _outstandingHTTPSRequests;

    void _finishPeerCommand(BedrockCommand& command);

    // When we're standing down, we temporarily dump newly received commands here (this lets all existing
    // partially-completed commands, like commands with HTTPS requests) finish without risking getting caught in an
    // endless loop of always having new unfinished commands.
    CommandQueue _standDownQueue;
};
