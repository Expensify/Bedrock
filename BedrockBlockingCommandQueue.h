#pragma once
#include <deque>
#include <memory>
#include <unordered_map>

#include "BedrockCommandQueue.h"

class BedrockCommand;

class BedrockBlockingCommandQueue : public BedrockCommandQueue {
public:
    BedrockBlockingCommandQueue();

    // Functions to start and stop timing on the commands when they're inserted/removed from the queue.
    static void startTiming(unique_ptr<BedrockCommand>& command);
    static void stopTiming(unique_ptr<BedrockCommand>& command);

    // Reject a command before enqueuing if any rate limiter is blocking it. Overrides
    // BedrockCommandQueue::push(). Throws SException("503 ...") when blocked; the caller catches and replies.
    void push(unique_ptr<BedrockCommand>&& command) override;

    // Clear the queue and all rate limiting state.
    void clear();

    // Clear all rate-limit states without emptying the queue. Returns the number of tracked accounts and commands cleared.
    size_t clearRateLimits();

    // Return a table of rate limiting status info for the Status command.
    STable getState();

    // Configure the sliding window and thresholds, all in microseconds. A threshold of 0 disables that
    // dimension. Each setter returns the previous value. The identifier and command dimensions share one
    // window and one block duration.
    uint64_t setSharedRateLimiterWindow(const uint64_t windowUS);
    uint64_t setSharedRateLimiterBlockDuration(const uint64_t durationUS);
    uint64_t setBlockingIdentifierThreshold(const uint64_t thresholdUS);
    uint64_t setBlockingCommandThreshold(const uint64_t thresholdUS);

    // The global rate limiter counts every command that runs on the blocking thread, so that a burst of commands with different
    // identifiers still trips it. It gets its own window, threshold and block duration because it
    // measures total saturation of the thread rather than one identifier's share of it.
    uint64_t setGlobalRateLimiterWindow(const uint64_t windowUS);
    uint64_t setGlobalRateLimiterThreshold(const uint64_t thresholdUS);
    uint64_t setGlobalRateLimiterBlockDuration(const uint64_t durationUS);

    // Record that a command finished on the blocking queue after `elapsedUS` of blocking time. Records the
    // sample against the global rate limiter, the command name, and the identifier when it is set.
    void recordExecutionTime(const string& identifier, const string& commandName, uint64_t elapsedUS);

    // Return the dimension blocking this command, or an empty string. The global rate limiter takes
    // precedence, then the identifier, then the command.
    string getBlockingDimension(const string& identifier, const string& commandName);

protected:
    // Dequeues a command and rejects it if any rate limiter is blocking it.
    // Called by `BedrockCommandQueue::get()` with the base `_queueMutex` held. Calling any base method that reacquires `_queueMutex` would deadlock.
    unique_ptr<BedrockCommand> _dequeue() override;

    // Current time in microseconds. Virtual so tests can control the clock.
    virtual uint64_t _now() const;

private:
    // A command that finished on the blocking queue. Both times are in microseconds.
    struct RecentlyFinishedCommand
    {
        uint64_t finishTime = 0;
        uint64_t elapsedTime = 0;
    };

    // An identifier's recently finished blocking-queue commands, oldest first.
    typedef deque<RecentlyFinishedCommand> RecentlyFinishedCommandList;

    // Rate-limit state for one dimension. Each entry has its own mutex, so different identifiers never contend
    // on one lock. `blockedUntil` is when an active block ends, in microseconds; 0 means not blocked.
    struct DimensionState
    {
        mutex m;
        RecentlyFinishedCommandList commands;
        uint64_t blockedUntil = 0;
    };

    // A map of identifier -> state plus the mutex guarding the map. Used once for identifiers and once for
    // command names. `mapMutex` guards only the map: hold it just long enough to find or insert an entry and
    // copy its shared_ptr, then release it and lock the entry's own `m` to do the work. The shared_ptr keeps
    // the entry alive if another thread erases it from the map between the two locks.
    struct StateMap
    {
        mutable mutex mapMutex;
        unordered_map<string, shared_ptr<DimensionState>> states;
    };

    // The tunables for one dimension, in microseconds. Each one is atomic because the blocking thread reads
    // them while SetBlockingQueueTimeRateLimit writes them from a worker. A `thresholdUS` of 0 disables the
    // dimension, and a `logThresholdUS` of 0 disables its log-only line.
    struct Limits
    {
        atomic<uint64_t> windowUS;
        atomic<uint64_t> thresholdUS;
        atomic<uint64_t> blockDurationUS;
        atomic<uint64_t> logThresholdUS;
    };

    // Return a shared_ptr to the state for `key` in `map`, creating it if absent. Holds map.mapMutex only briefly.
    static shared_ptr<DimensionState> _getOrCreateState(StateMap& map, const string& key);

    // Return the state for `key` in `map`, or nullptr if absent. Holds map.mapMutex only briefly.
    static shared_ptr<DimensionState> _getState(StateMap& map, const string& key);

    // Append a sample that finished at `now` after `elapsedUS` to `state`, then block it for the block
    // duration when its windowed time exceeds the threshold. `dimension` and `key` label the log line. Reads
    // `limits` once up front so a concurrent retune can't change the window partway through. This is the
    // O(window) work; it never runs under the base `_queueMutex`.
    static void _recordAndCheck(DimensionState& state, const string& dimension, const string& key, const Limits& limits, uint64_t now, uint64_t elapsedUS);

    // True if `state` is inside an active block at `now`. O(1): reads only the block deadline, so the push and
    // dequeue hot paths stay cheap (dequeue runs under the base `_queueMutex`).
    static bool _isBlocked(DimensionState& state, uint64_t now);

    // Log an identifier or command that is over this but under its block threshold, so heavy ones are visible
    // before they get blocked.
    static constexpr uint64_t LOG_THRESHOLD_US = 10'000'000; // 10 seconds

    static constexpr uint64_t GLOBAL_THRESHOLD_US = 55'000'000; // 55 seconds

    // Every command counts toward the global rate limiter, so the fixed threshold above would log on almost
    // all of them. It logs from this share of its own threshold instead.
    static constexpr uint64_t GLOBAL_LOG_PERCENT = 80;

    StateMap _identifierStates;
    StateMap _commandStates;

    // The global rate limiter has no key, so it needs one state rather than a map of them.
    DimensionState _globalState;

    // setSharedRateLimiterWindow() and setSharedRateLimiterBlockDuration() write both of the first two, which
    // is what SetBlockingQueueTimeRateLimit exposes.
    Limits _identifierLimits{180'000'000, 20'000'000, 60'000'000, LOG_THRESHOLD_US};
    Limits _commandLimits{180'000'000, 40'000'000, 60'000'000, LOG_THRESHOLD_US};
    Limits _globalLimits{60'000'000, GLOBAL_THRESHOLD_US, 60'000'000, (GLOBAL_THRESHOLD_US * GLOBAL_LOG_PERCENT) / 100};
};
