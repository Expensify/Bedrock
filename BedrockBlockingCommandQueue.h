#pragma once
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

    // Set the max commands per identifier threshold. Returns the previous value.
    size_t setMaxRequestsPerIdentifier(size_t value);

    // Set the max accumulated worker-0 execution time (microseconds) per identifier. Returns the previous value.
    uint64_t setMaxTimePerIdentifier(uint64_t valueUS);

    // Accumulate elapsed worker-0 execution time for `identifier`. Called by the blockingCommit worker
    // after each command finishes running. No-op when the time threshold is disabled (== 0). Time is
    // cumulative per identifier until the empty-queue reset clears it — it is never decremented per command.
    void recordExecutionTime(const string& identifier, uint64_t elapsedUS);

protected:
    // Called by get() while _queueMutex is held; atomically decrements per-identifier counts
    // and records when the queue becomes empty.
    unique_ptr<BedrockCommand> _dequeue() override;

private:
    // Decrement the count for `identifier` in `_identifierCounts`, erasing the entry if it reaches zero.
    // Caller must hold `_rateLimitMutex`.
    void _decrementIdentifierCount(const string& identifier);

    // Guards `_identifierCounts`. Separate from the base class `_queueMutex` because the base
    // mutex is non-recursive and is held while `_dequeue` runs.
    mutex _rateLimitMutex;

    map<string, size_t> _identifierCounts;
    map<string, uint64_t> _identifierTimes;
    atomic<size_t> _maxPerIdentifier{10};
    atomic<uint64_t> _maxTimePerIdentifier{60'000'000}; // 60 seconds, in microseconds
    atomic<uint64_t> _maxTimePerIdentifierToLog{10'000'000}; // 10 seconds, in microseconds
    atomic<uint64_t> _emptyTime{0};
};
