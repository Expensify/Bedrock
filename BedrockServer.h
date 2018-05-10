#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLiteServer.h>
#include "BedrockPlugin.h"
#include "BedrockCommandQueue.h"

class BedrockServer : public SQLiteServer {
  public:

    // Shutting Down and Standing Down a BedrockServer.
    //
    // # What's the difference between these two things?
    //
    // Shutting down is pretty obvious - when we want to turn a server off, we shut it down. The shut down process
    // tries to do this without interrupting any client requests.
    // Standing down is a little less obvious. If we're the master node in a cluster, there are two ways to stand down.
    // The first is that we are shutting down, in which case we'll need to let the rest of the cluster know that it
    // will need to pick a new master. The other is if a higher-priority master asks to stand up in our place. In this
    // second case, we'll stand down without shutting down.
    //
    //
    // # Shutting Down
    // Let's start with shutting down. Standing down is a subset of shutting down (when we start out mastering), and
    // we'll get to that later.
    //
    // When a BedroskServer comes up, it's _shutdownState is RUNNING. This is the normal operational state. When the
    // server receives a signal, that state changes to START_SHUTDOWN. This change causes a couple things to happen:
    //
    // 1. The command port is closed, and no new connections are accepted from clients.
    // 2. When we respond to commands, we add a `Connection: close` header to them, and close the socket after the
    //    response is sent.
    // 3. We set timeouts for any HTTPS commands to five seconds. Note that if we are slaving, this has no effect on
    //    commands that were escalated to master, we continue waiting for those commands.
    //
    // The server then continues operating as normal until there are no client connections left (because we've stopped
    // accepting them, and closed any connections as we responded to their commands), at which point it switches to
    // CLIENTS_RESPONDED. The sync node notices this, and on it's next update() loop, it begins shutting down. If it
    // was mastering, the first thing it does in this case is switch to STANDINGDOWN. See more on standing down in the
    // next section.
    //
    // The sync node will continue STANDINGDOWN until two conditions are true:
    //
    // 1. There are no commands in progress.
    // 2. There are no outstanding queued commands.
    //
    // A "command in progress" is essentially any command that we have not completed but is not in our main command
    // queue. We increment the count of these commands when we dequeue a command from the main queue, and decrememnt it
    // when we respond to the command (and also in a few other exception cases where the command is abandoned or does
    // not require a response). This means that if a command has been moved to the queue of outstanding HTTPS commands,
    // or the sync thread queue, or is currently being handled by a worker, or escalated to master, it's "in progress".
    //
    // If we were not mastering, there is no STANDINGDOWN state to wait through - all of a slaves commands come from
    // local clients, and once those connections are all closed, then that means every command has been responded to,
    // implying that there are neither queued commands nor commands in progress.
    //
    // The sync node then finishes standing down, informing the other nodes in the cluster that it is now in the
    // SEARCHING state. At this point, we switch to the DONE state. The sync thread waits for worker threads to join.
    // The worker threads, when they find the main command queue empty, will check for the DONE state, and return. The
    // sync thread can then complete and the server is shut down.
    //
    //
    // # Standing Down
    // Standing down when shutting down is covered in the above section, but there's an additional bit of work to do of
    // we're standing down without shutting down. The main difference is that we are not waiting on all existing
    // clients to be disconnected while we stand down - we will be able to service these same clients as a slave as
    // soon as we finish this operation. However, we still have the same criteria for STANDINGDOWN as listed above, no
    // commands in progress, and an empty command queue.
    //
    // When a node switches to STANDINGDOWN, it starts rejecting escalated commands from peers. This allows it to run
    // through its current list of pending escalated commands and finish them all (this is true whether we're shutting
    // down or not, and it's why shutting down is able to complete the entire queue of commands without it potentially
    // adding new commands that keep it from ever emptying). However, this doesn't keep new commands that arrive
    // locally from being added to main queue. This could potentially keep us from ever finishing the queue. So, when
    // we are in a STANDINGDOWN state, any new commands from local clients are inserted into a temporary
    // `_standDownQueue` instead of the main queue. As soon as STANDINGDOWN completes, these commands will be moved
    // back to the main queue.
    //
    // The reason we have to wait for the main queue to be empty *and* no commands to be in progress when standing
    // down, is because of the way certain commands can move between queues. For instance, a command that has a pending
    // HTTPS transaction is "in progress" until the transaction completes, but then re-queued for processing by a
    // worker thread. If we allowed commands to remain in the main queue while standing down, some of them could be
    // HTTPS commands with completed requests. If these didn't get processed until after the node finished standing
    // down, then we'd try and run processCommand() while slaving, which would be invalid. For this reason, we need to
    // make sure any command that has ever been started gets completed before we finish standing down. Unfortunately,
    // once a command has been added to the main queue, there's no way of knowing whether it's ever been started
    // without inspecting every command in the queue, hence the `_standDownQueue`.
    //
    // NOTE: in both cases of shutting down or standing down, we discard any commands in the main queue that are
    // scheduled to happen more than 5 seconds in the future. In the case that we're shutting down, there's nothing we
    // can do about these, except not shut down. If we're standing down, we could keep them, but they break our
    // checking against whether the main command queue is empty, and fall into the same category of us being unable to
    // distinguish them from commands that may have already started.
    //
    // Notes on timing out a shutdown.
    // Here's how timing out a shutdown works:
    // 1. When _beginShutdown() is called, it sets a one minute timer. It proceeds to shut down the node as normal.
    // 2. When shutdown progresses far enough that we can shut down the sync node, we set the timeout for the sync node
    // to whatever portion of our minute is remaining (minus 5 seconds, to allow for final cleanup afterward - also, we
    // make sure this value stays positive, so will always be at least 1us).
    // 3. The sync node should finish with at least 5 seconds left, and we should finish any final cleanup or responses
    // and shut down cleanly.
    //
    // However, if we do get to the end of our 1 minute timer, everything just starts exiting. Worker threads return,
    // whether there's more work to do or not. The sync thread drops out of it's main loop and starts joining workers.
    // This leaves us in a potentially broken state, and hitting the timeout should count as an error condition, but we
    // need to support it for now. Here's the case that should look catastrophic, but is actually quite common, and what
    // we should eventually do about it:
    //
    // A slave begins shutdown, and sets a 60 second timeout. It has escalated commands to master. It wants to wait for
    // the responses to these commands before it finishes shutting down, but *there is no timeout on escalated
    // commands*. Normally, we won't try to shut down the sync node until we've responded to all connected clients.
    // Because there will always be connected clients waiting for these responses to escalated commands, we'll wait the
    // full 60 seconds, and then we'll just die with no responses. Effectively, the sever `kill -9`'s itself here,
    // leaving clients hanging with no cleanup.
    //
    // On master, this state could be catastrophic, though master doesn't need to worry about a lack of timeouts on
    // escalations, so let's look at a different case - a command running a custom query that takes longer than our 60
    // second timeout. There will be a local client waiting for the response to this command, so the same criteria
    // breaks - we can't shut down the sync thread until it's complete, but the command it's waiting for doesn't return
    // until after our timeout. This is *after* everything just starts exiting, which is to say the sync node could
    // have shut down due to hitting the timeout before a worker thread shut down performing a write command. This
    // write will be orphaned, as the sync node will not be able to send it out, and this database is then forked.
    //
    // We can't safely start shutting down the sync node until we know that all possible writes are complete for this
    // reason, but if we're going to enforce a timeout, then we need to.
    //
    // The only way to make this timeout safe is to make sure no individual command can live longer than our shutdown
    // timeout, which would require time-stamping commands pre-escalation and giving up on them if we failed to receive
    // a response in time, as well as making sure that commands always return in less time than that (they currently,
    // usually will, as long as the default command timeout is less than one minute, but they can still block in system
    // calls like sleep(), and the timeout is configurable). Certainly, no "normal" command (i.e., a programmatically
    // generated command, as opposed to something like a CustomQuery command) should have a timeout longer than the
    // shutdown timeout, or it can cause a non-graceful shutdown.
    //
    // Of course, if nothing can take longer than the shutdown timeout, then we should never hit that timeout, and all
    // our failures should be limited to individual commands rather than the entire server shutting down.

    // Shutdown states.
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
    // `isNew` will be set to true if this command has never been seen before, and false if this is an existing command
    // being returned to the command queue (such as one that was previously escalated).
    // SQLiteNode API.
    void acceptCommand(SQLiteCommand&& command, bool isNew = true);

    // Cancel a command.
    // SQLiteNode API.
    void cancelCommand(const string& commandID);

    // Flush the send buffers
    // STCPNode API.
    void prePoll(fd_map& fdm);

    // Accept connections and dispatch requests
    // STCPNode API.
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Returns true when everything's ready to shutdown.
    bool shutdownComplete();

    // Exposes the replication state to plugins.
    SQLiteNode::State getState() const { return _replicationState.load(); }

    // When a peer node logs in, we'll send it our crash command list.
    void onNodeLogin(SQLiteNode::Peer* peer);

    // Control the command port. The server will toggle this as necessary, unless manualOverride is set,
    // in which case the `suppress` setting will be forced.
    void suppressCommandPort(const string& reason, bool suppress, bool manualOverride = false);

    // This will return true if there's no outstanding writable activity that we're waiting on. It's called by an
    // SQLiteNode in a STANDINGDOWN state to know that it can switch to searching.
    virtual bool canStandDown();

    // Returns whether or not this server was configured to backup.
    bool shouldBackup();

    // Returns a copy of the internal state of the sync node's peers. This can be empty if there are no peers, or no
    // sync node.
    list<STable> getPeerInfo();

    // Send a command to all of our peers. It will be wrapped appropriately.
    void broadcastCommand(const SData& message);

    // Set the detach state of the server. Setting to true will cause the server to detach from the database and go
    // into a sleep loop until this is called again with false
    void setDetach(bool detach);

    // Returns if we are detached and the sync thread has exited.
    bool isDetached();

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
    // open. It will be re-inserted in this set when another command is read from it.
    map <uint64_t, Socket*> _socketIDMap;

    // The above _socketIDMap is modified by multiple threads, so we lock this mutex around operations that access it.
    // We don't need to lock around access to the base class's `socketList` because we carefully control access to it
    // to the main thread.
    // The only functions that access `socketList` are prePoll, postPoll, openSocket, and closeSocket, in STCPManager,
    // and acceptSocket in STCPServer.
    // prePoll and postPoll are only ever called by the main thread.
    // openSocket is never called by bedrockServer (it is called in SHTTPSManager and STCPNode).
    // closeSocket and acceptSocket are only called inside postPoll.
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
    atomic<bool> _syncThreadComplete;

    // Give all of our plugins a chance to verify and/or modify the database schema. This will run every time this node
    // becomes master. It will return true if the DB has changed and needs to be committed.
    bool _upgradeDB(SQLite& db);

    // Iterate across all of our plugins and call `prePoll` and `postPoll` on any httpsManagers they've created.
    void _prePollPlugins(fd_map& fdm);
    void _postPollPlugins(fd_map& fdm, uint64_t nextActivity);

    // Resets the server state so when the sync node restarts it is as if the BedrockServer object was just created.
    void _resetServer();

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
    atomic<bool> _detach;

    // Pointers to the ports on which we accept commands.
    Port* _controlPort;
    Port* _commandPort;

    // The maximum number of conflicts we'll accept before forwarding a command to the sync thread.
    atomic<int> _maxConflictRetries;

    // This is a map of HTTPS requests to the commands that contain them. We use this to quickly look up commands when
    // their HTTPS requests finish and move them back to the main queue.
    map<SHTTPSManager::Transaction*, BedrockCommand> _outstandingHTTPSRequests;
    mutex _httpsCommandMutex;

    // Send a reply to a command that was escalated to us from a peer, rather than a locally-connected client.
    void _finishPeerCommand(BedrockCommand& command);

    // When we're standing down, we temporarily dump newly received commands here (this lets all existing
    // partially-completed commands, like commands with HTTPS requests) finish without risking getting caught in an
    // endless loop of always having new unfinished commands.
    CommandQueue _standDownQueue;

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
};
