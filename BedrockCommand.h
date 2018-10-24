#pragma once
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
        PEEK,
        PROCESS,
        COMMIT_WORKER,
        COMMIT_SYNC,
        QUEUE_WORKER,
        QUEUE_SYNC,
    };

    // Times in *milliseconds*.
    static const uint64_t DEFAULT_TIMEOUT = 290'000; // 290 seconds, so clients can have a 5 minute timeout.
    static const uint64_t DEFAULT_TIMEOUT_FORGET = 60'000 * 60; // 1 hour for `connection: forget` commands.
    static const uint64_t DEFAULT_PROCESS_TIMEOUT = 30'000; // 30 seconds.

    // Constructor to make an empty object.
    BedrockCommand();

    // Constructor to convert from an existing SQLiteCommand (by move).
    BedrockCommand(SQLiteCommand&& from);

    // Move constructor.
    BedrockCommand(BedrockCommand&& from);

    // Constructor to initialize via a request object (by move).
    BedrockCommand(SData&& _request);

    // Constructor to initialize via a request object (by copy).
    BedrockCommand(SData _request);

    // Destructor.
    ~BedrockCommand();

    // Move assignment operator.
    BedrockCommand& operator=(BedrockCommand&& from);

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

    // We track how many times we `peek` and `process` each command.
    int peekCount;
    int processCount;

    // Keep track of who peeked and processed this command.
    BedrockPlugin* peekedBy;
    BedrockPlugin* processedBy;

    // A list of timing sets, with an info type, start, and end.
    list<tuple<TIMING_INFO, uint64_t, uint64_t>> timingInfo;

    // This defaults to false, but a specific plugin can set it to 'true' in peek() to force this command to be passed
    // to the sync thread for processing, thus guaranteeing that process() will not result in a conflict.
    bool onlyProcessOnSyncThread;

    // This is a set of name/value pairs that must be present and matching for two commands to compare as "equivalent"
    // for the sake of determining whether they're likely to cause a crash.
    // i.e., if this command has set this to {userID, reportList}, and the server crashes while processing this
    // command, then any other command with the same methodLine, userID, and reportList will be flagged as likely to
    // cause a crash, and not processed.
    set<string> crashIdentifyingValues;

    // TODO: Change this name?
    // This value indicates that this command has timed out and should not be responded to.
    atomic<bool> dead;

    // Return the timestamp by which this command must finish executing.
    uint64_t timeout() const { return _timeout; }

    // Returns the next timed out command that exists for the given timestamp, or throws std::out_of_range if there are
    // none. This is returned by copying the existing command and marking the existing one as dead.
    // There's no copy assignment operator, but maybe I need one.
    // What we want to do is:
    // Lock the timeout mutex. This prevents the command being destroyed.
    // Copy the command.
    // Mark the original command as dead.
    // Return the copy.
    // Unlock.
    // I n this special case, we probably *don't* want this command to exist in the timeout map, since it's already
    // timed out.
    // We also have to decide how to handle https requests. Are they transferred to the timed out command? Probably.
    static BedrockCommand getTimedOutCommand(uint64_t timestamp);

  private:
    // Set certain initial state on construction. Common functionality to several constructors.
    void _init();

    // used as a temporary variable for startTiming and stopTiming.
    tuple<TIMING_INFO, uint64_t, uint64_t> _inProgressTiming;

    // We store a map of timeouts to commands that need to timeout at that time.
    static mutex timeoutMutex;
    static multimap<uint64_t, const BedrockCommand*> commandTimeouts;
    
    // Let commands register themselves in the global list of command timeouts. All constructors for a command need to
    // call this.
    static void _addCommandTimeout(const BedrockCommand* cmd);

    // Get the absolute timeout value for this command based on it's request. This is used to initialize _timeout.
    static int64_t _getTimeout(const SData& request);

    // This is a timestamp in *microseconds* for when this command should timeout.
    uint64_t _timeout;
};
