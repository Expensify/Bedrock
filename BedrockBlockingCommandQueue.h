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

    // Reject a command before enqueuing if its identifier or command name is rate limited. Overrides
    // BedrockCommandQueue::push(). Throws SException("503 ...") when blocked; the caller catches and replies.
    void push(unique_ptr<BedrockCommand>&& command) override;

    // Clear the queue and all rate limiting state.
    void clear();

    // Clear all rate-limit states without emptying the queue. Returns the number of tracked accounts and commands cleared.
    size_t clearRateLimits();

    // Return a table of rate limiting status info for the Status command.
    STable getState();

    // Configure the sliding window and thresholds, all in microseconds. A threshold of 0 disables that
    // dimension. Each setter returns the previous value.
    uint64_t setWindow(const uint64_t windowUS);
    uint64_t setIdentifierThreshold(const uint64_t thresholdUS);
    uint64_t setCommandThreshold(const uint64_t thresholdUS);
    uint64_t setBlockDuration(const uint64_t durationUS);

    // Record that a command finished on the blocking queue after `elapsedUS` of blocking time. Records the
    // sample against the identifier (when `identifier` is non-empty) and against the command name.
    void recordExecutionTime(const string& identifier, const string& commandName, uint64_t elapsedUS);

    // Return the active rate-limit dimension, or an empty string. Identifier blocks take precedence when both dimensions are active.
    string getBlockingDimension(const string& identifier, const string& commandName);

protected:
    // Dequeues a command and rejects it if its identifier or command name is rate limited.
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

    // Rate-limit state for one dimension: one identifier or one command name. Each entry has its own mutex, so
    // different identifiers never contend on one lock. `blockedUntil` is the time (microseconds) an active
    // block ends; 0 means not blocked.
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

    // The tunables for one dimension, in microseconds. A `thresholdUS` of 0 disables the dimension. A
    // `logThresholdUS` of 0 disables the log-only line for identifiers that are heavy but under the threshold.
    struct Limits
    {
        uint64_t windowUS = 0;
        uint64_t thresholdUS = 0;
        uint64_t blockDurationUS = 0;
        uint64_t logThresholdUS = 0;
    };

    // Return a shared_ptr to the state for `key` in `map`, creating it if absent. Holds map.mapMutex only briefly.
    static shared_ptr<DimensionState> _getOrCreateState(StateMap& map, const string& key);

    // Return the state for `key` in `map`, or nullptr if absent. Holds map.mapMutex only briefly.
    static shared_ptr<DimensionState> _getState(StateMap& map, const string& key);

    // Append a sample that finished at `now` after `elapsedUS` to `state`, then re-evaluate the window and
    // block for `limits.blockDurationUS` when the windowed time exceeds `limits.thresholdUS`. `dimension` and
    // `key` label the log line. This is the O(window) work; it never runs under the base `_queueMutex`.
    static void _recordAndCheck(DimensionState& state, const string& dimension, const string& key, const Limits& limits, uint64_t now, uint64_t elapsedUS);

    // True if `state` is inside an active block at `now`. O(1): reads only the block deadline, so the push and
    // dequeue hot paths stay cheap (dequeue runs under the base `_queueMutex`).
    static bool _isBlocked(DimensionState& state, uint64_t now);

    // Log (without blocking) when an identifier's windowed time crosses this, for monitoring heavy identifiers
    // that are still under their block threshold.
    static constexpr uint64_t LOG_THRESHOLD_US = 10'000'000; // 10 seconds

    StateMap _identifierStates;
    StateMap _commandStates;

    atomic<uint64_t> _windowUS{180'000'000};          // 180 seconds
    atomic<uint64_t> _identifierThresholdUS{20'000'000}; // 20 seconds
    atomic<uint64_t> _commandThresholdUS{40'000'000}; // 40 seconds
    atomic<uint64_t> _blockDurationUS{60'000'000};    // 60 seconds
};
