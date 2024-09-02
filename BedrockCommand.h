#pragma once
#include <libstuff/SHTTPSManager.h>
#include <sqlitecluster/SQLiteCommand.h>

class BedrockPlugin;

class BedrockCommand : public SQLiteCommand {
  public:
    enum Priority {
        PRIORITY_MIN = 0,
        PRIORITY_LOW = 250,
        PRIORITY_NORMAL = 500,
        PRIORITY_HIGH = 750,
        PRIORITY_MAX = 1000
    };

    enum TIMING_INFO {
        INVALID,
        PREPEEK,
        PEEK,
        PROCESS,
        POSTPROCESS,
        COMMIT_WORKER,
        COMMIT_SYNC,
        QUEUE_WORKER,
        QUEUE_SYNC,
        QUEUE_BLOCKING,
        QUEUE_PAGE_LOCK,

        // Time spent in the blockingCommit thread (not the same as "commit lock time")
        BLOCKING_PREPEEK,
        BLOCKING_PEEK,
        BLOCKING_PROCESS,
        BLOCKING_POSTPROCESS,
        BLOCKING_COMMIT_WORKER,
    };

    enum class STAGE {
        PREPEEK,
        PEEK,
        PROCESS,
        POSTPROCESS
    };

    // Times in *milliseconds*.
    static const uint64_t DEFAULT_TIMEOUT = 110'000; // 110 seconds, so clients can have a 2 minutes timeout.
    static const uint64_t DEFAULT_TIMEOUT_FORGET = 60'000 * 60; // 1 hour for `connection: forget` commands.
    static const uint64_t DEFAULT_PROCESS_TIMEOUT = 30'000; // 30 seconds.
    static const uint64_t DEFAULT_BLOCKING_TRANSACTION_COMMIT_LOCK_TIMEOUT = 10'000; // 10 seconds.

    // Constructor to initialize via a request object (by move).
    BedrockCommand(SQLiteCommand&& baseCommand, BedrockPlugin* plugin, bool escalateImmediately_ = false);

    // Destructor.
    virtual ~BedrockCommand();

    // Optionally called to execute read-only operations for a command in a separate transaction than the transaction
    // that can execute write operations for a command (i.e. the transaction that runs peek and process). This must be
    // called before the transaction that executes write operations.
    virtual void prePeek(SQLite& db) { STHROW("500 Base class prePeek called"); }

    // Called to attempt to handle a command in a read-only fashion. Should return true if the command has been
    // completely handled and a response has been written into `command.response`, which can be returned to the client.
    // Should return `false` if the command needs to write to the database or otherwise could not be finished in a
    // read-only fashion (i.e., it opened an HTTPS request and is waiting for the response).
    virtual bool peek(SQLite& db) { STHROW("430 Unrecognized command"); }

    // Called after a command has returned `false` to peek, and will attempt to commit and distribute a transaction
    // with any changes to the DB made by this plugin.
    virtual void process(SQLite& db) { STHROW("500 Base class process called"); }

    // Optionally called to execute read-only operations for a command in a separate transaction than the transaction
    // that can execute write operations for a command (i.e. the transaction that runs peek and process). This must be
    // called after the transaction that executes write operations.
    virtual void postProcess(SQLite& db) { STHROW("500 Base class postProcess called"); }

    // Reset the command after a commit conflict. This is called both before `peek` and `process`. Typically, we don't
    // want to reset anything in `process`, because we may have specifically stored values there in `peek` that we want
    // to access later. However, we provide this functionality to allow commands that make HTTPS requests to handle
    // this extra case, as we run `peek` and `process` as separate transactions for these commands.
    // The base class version of this does *not* change anything with regards to HTTPS requests. These are preserved
    // across `reset` calls.
    virtual void reset(STAGE stage);

    // Return the name of the plugin for this command.
    const string& getName() const;

    // Take all of the HTTPS requests attached to this object, and serialize them to a string.
    string serializeHTTPSRequests();

    // Take a serialized list of HTTPS requests as from `serializeHTTPSRequests` and deserialize them into the `httpsRequests` object.
    void deserializeHTTPSRequests(const string& serializedHTTPSRequests);

    // Bedrock will call this before each `processCommand` (note: not `peekCommand`) for each plugin to allow it to
    // enable query rewriting. If a plugin would like to enable query rewriting, this should return true, and it should
    // set the rewriteHandler it would like to use.
    virtual bool shouldEnableQueryRewriting(const SQLite& db, bool (**rewriteHandler)(int, const char*, string&)) {
        return false;
    }

    // Bedrock will call this before writing to the database after it has prepared a transaction for each plugin to allow it to
    // enable a handler function for prepare If a plugin would like to perform operations after prepare but before commit, this should 
    // return true, and it should set the prepareHandler it would like to use.
    virtual bool shouldEnableOnPrepareNotification(const SQLite& db, void (**onPrepareHandler)(SQLite& _db, int64_t tableID)) {
        return false;
    }

    // Start recording time for a given action type.
    void startTiming(TIMING_INFO type);

    // Finish recording time for a given action type. `type` must match what was passed to the most recent call to
    // `startTiming`.
    void stopTiming(TIMING_INFO type);

    // Add a summary of our timing info to our response object.
    void finalizeTimingInfo();

    // Returns true if all of the httpsRequests for this command are complete (or if it has none).
    bool areHttpsRequestsComplete() const;

    // If the `peek` portion of this command needs to make an HTTPS request, this is where we store it.
    list<SHTTPSManager::Transaction*> httpsRequests;

    // Each command is assigned a priority.
    Priority priority;

    // We track how many times we `prePeek`, `peek` and `process` each command.
    int prePeekCount;
    int peekCount;
    int processCount;
    int postProcessCount;

    // A plugin can optionally handle a command for which the reply to the caller was undeliverable.
    // Note that it gets no reference to the DB, this happens after the transaction is already complete.
    virtual void handleFailedReply() {
        // Default implementation does nothing.
    }

    // Set to true if we don't want to log timeout alerts, and let the caller deal with it.
    virtual bool shouldSuppressTimeoutWarnings() { return false; }

    virtual bool shouldPrePeek() { return false; }

    virtual bool shouldPostProcess() { return false; }

    // A command can set this to true to indicate it would like to have `peek` called again after completing a HTTPS
    // request. This allows a single command to make multiple serial HTTPS requests. The command should clear this when
    // all HTTPS requests are complete. It will be automatically cleared if the command throws an exception.
    bool repeek;

    // A list of timing sets, with an info type, start, and end.
    list<tuple<TIMING_INFO, uint64_t, uint64_t>> timingInfo;

    // Add any sockets that this command has opened (not the socket the client sent it on, but any outgoing sockets
    // it's opened itself) to a fd_map so that they can be polled for activity.
    void prePoll(fd_map& fdm);

    // Handle any activity on those sockets that was noted in poll.
    void postPoll(fd_map& fdm, uint64_t nextActivity, uint64_t maxWaitMS);

    // This is a set of name/value pairs that must be present and matching for two commands to compare as "equivalent"
    // for the sake of determining whether they're likely to cause a crash.
    // i.e., if this command has set this to {userID, reportList}, and the server crashes while processing this
    // command, then any other command with the same methodLine, userID, and reportList will be flagged as likely to
    // cause a crash, and not processed.
    class CrashMap : public map<string, SString> {
      public:
        pair<CrashMap::iterator, bool> insert(const string& key) {
            if (cmd.request.isSet(key)) {
                return map<string, SString>::insert(make_pair(key, cmd.request.nameValueMap.at(key)));
            }
            return make_pair(end(), false);
        }

      private:
        // We make BedrockCommand a friend so it can call our private constructors/assignment operators.
        friend class BedrockCommand;
        CrashMap(BedrockCommand& _cmd) : cmd(_cmd) { }
        CrashMap(BedrockCommand& _cmd, CrashMap&& other) : map<string, SString>(move(other)), cmd(_cmd) { }
        CrashMap& operator=(CrashMap&& other) {
            map<string, SString>::operator=(move(other));
            return *this;
        }

        // This is a reference to the command that created this object.
        BedrockCommand& cmd;
    };
    CrashMap crashIdentifyingValues;

    virtual bool shouldGenerateCrashCommand();

    // Return the timestamp by which this command must finish executing.
    uint64_t timeout() const { return _timeout; }

    // This updates the timeout for this command to the specified number of milliseconds from the current time.
    void setTimeout(uint64_t timeoutDurationMS);

    // Return the number of commands in existence.
    static size_t getCommandCount() { return _commandCount.load(); }

    virtual string serializeData() const;
    virtual void deserializeData(const string& data);

    // True if this command should be escalated immediately. This can be true for any command that does all of its work
    // in `process` instead of peek, as it will always be escalated to leader
    const bool escalateImmediately;

    // If someone is waiting for this command to complete, this will be called in the destructor.
    function<void()>* destructionCallback;

    // The socket that this command was read from. Can be null if the command didn't come from a client socket (i.e.,
    // it was escalated to leader or generated internally) or if it was a `fire and forget` command for which no client
    // is awaiting a reply.
    STCPManager::Socket* socket;

    // Time at which this command was initially scheduled (typically the time of creation).
    const uint64_t scheduledTime;

    // Returns _commitEmptyTransactions.
    bool shouldCommitEmptyTransactions() const;

  protected:
    // The plugin that owns this command.
    BedrockPlugin* _plugin;

    // Commands can set this flag to indicate they want the commit process to be run even though it doesn't appear that any writes have occurred.
    // The main use of this is to cause commands that use SQLite::onPrepareHandler to do additional writing to run these final writes.
    bool _commitEmptyTransactions;

  private:
    // Set certain initial state on construction. Common functionality to several constructors.
    void _init();

    // used as a temporary variable for startTiming and stopTiming.
    tuple<TIMING_INFO, uint64_t, uint64_t> _inProgressTiming;

    // Get the absolute timeout value for this command based on it's request. This is used to initialize _timeout.
    static int64_t _getTimeout(const SData& request, const uint64_t scheduledTime);

    // This is a timestamp in *microseconds* for when this command should timeout.
    uint64_t _timeout;

    set<string> _tablesUsed;

    static atomic<size_t> _commandCount;

    static SStandaloneHTTPSManager _noopHTTPSManager;

    static const string defaultPluginName;

    // This refers to the last request in httpsRequests for which all previous requests (including the one referred to) are known to be complete.
    // For instance, if there are 10 requests (0-9) in httpsRequests, and 0, 1, 2, 5, and 8 are complete, this can refer to request 2, the last one completed
    // for which all previous requests are also completed.
    // This is only updated when `areHttpsRequestsComplete()` is called, hence "known to be complete". Requests may have completed that are not yet known, but no
    // requests can exist before this iterator that are incomplete.
    // This is used as an optimization. For some operations (areHttpsRequestsComplete, prePoll, postPoll) we iterate across httpsReqeusts, but only actually need to iterate
    // across httpsRequests that have not yet finished. This allows us to skip known finished requests. If a command has 1,000 attached requests, and on the previous loop
    // it was known that 990 of them had completed, then on the next loop we can use this to check only the remaining 10 requests rather than all 1,000.
    //
    // The default value of this is httpsRequests.end(), which is treated as "no requests completed".
    mutable list<SHTTPSManager::Transaction*>::const_iterator _lastContiguousCompletedTransaction;
};
