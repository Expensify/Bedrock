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

    // Enforce per-identifier rate limits before enqueuing. Overrides BedrockCommandQueue::push().
    // Throws SException("503 ...") if the identifier is rate limited; caller should catch and reply.
    //
    // Rate limit state auto-resets when the queue has been continuously empty for 30 seconds — this
    // is a global reset, not per-identifier. A brief drain (even one dequeue that empties the queue)
    // starts the timer and will unblock all identifiers once 30 seconds elapse. This is intentional:
    // the primary threat is a sustained burst from one identifier, so a quiet period is a safe signal
    // to restore normal operation.
    void push(unique_ptr<BedrockCommand>&& command) override;

    // Clear the queue and all rate limiting state.
    void clear();

    // Reset rate limit counters without emptying the queue. Returns the number of tracked identifiers cleared.
    size_t clearRateLimits();

    // Return a table of rate limiting status info for the Status command.
    STable getState();

    // Set the max accumulated worker-0 execution time (microseconds) per identifier. Returns the previous value.
    uint64_t setMaxTimePerIdentifier(uint64_t valueUS);

    // Accumulate elapsed worker-0 execution time for `identifier`. Called by the blockingCommit worker
    // after each command finishes running. No-op when the time threshold is disabled (== 0). Time is
    // cumulative per identifier until the empty-queue reset clears it — it is never decremented per command.
    void recordExecutionTime(const string& identifier, uint64_t elapsedUS);

    // Check the identifier's accumulated blocking queue execution time against our thresholds, logging a tracking
    // message and returning true if the threshold is exceeded. Returns false if the identifier is not over the limit.
    bool isIdentifierOverTimeLimit(const string& identifier, const string& methodLine);

protected:
    // Called by get() while _queueMutex is held; records when the queue becomes empty and rejects a
    // dequeued command whose identifier is over the time limit.
    unique_ptr<BedrockCommand> _dequeue() override;

private:
    // One command an identifier finished on the blocking queue. `finishTime` is when it finished.
    // `elapsedTime` is how long it ran there. Both are in microseconds.
    struct RecentlyFinishedCommand
    {
        uint64_t finishTime = 0;
        uint64_t elapsedTime = 0;
    };

    // An identifier's recently finished blocking-queue commands, oldest first.
    typedef deque<RecentlyFinishedCommand> RecentlyFinishedCommandList;

    // Per-identifier rate-limit state. Each entry has its own mutex, so different identifiers never
    // contend on one lock.
    //
    // `_identifiersMutex` guards only the map. Hold it just long enough to find or insert an entry and
    // copy its shared_ptr. Then release it and lock the entry's own `m` to do the work.
    //
    // The shared_ptr keeps the entry alive if another thread erases it from the map between the two locks.
    struct IdentifierState
    {
        mutex m;
        RecentlyFinishedCommandList commands;
    };

    // Return a shared_ptr to the state for `identifier`, creating it if absent. Hold `_identifiersMutex` only briefly.
    shared_ptr<IdentifierState> _getOrCreateIdentifierState(const string& identifier);

    // Guards `_identifierTimes`. Separate from the base class `_queueMutex` because the base
    // mutex is non-recursive and is held while `_dequeue` runs.
    mutex _rateLimitMutex;

    mutable mutex _identifiersMutex;
    unordered_map<string, shared_ptr<IdentifierState>> _identifiers;

    map<string, uint64_t> _identifierTimes;
    atomic<uint64_t> _maxTimePerIdentifier{60'000'000}; // 60 seconds, in microseconds
    atomic<uint64_t> _maxTimePerIdentifierToLog{10'000'000}; // 10 seconds, in microseconds
    atomic<uint64_t> _emptyTime{0};
};
