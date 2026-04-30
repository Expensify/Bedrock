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

    // Reset rate limit counters without emptying the queue. Returns the number of unblocked identifiers.
    size_t clearRateLimits();

    // Return a table of rate limiting status info for the Status command.
    STable getState();

    // Set the max commands per identifier threshold. Returns the previous value.
    size_t setMaxPerIdentifier(size_t value);

protected:
    // Called by get() while _queueMutex is held; atomically decrements per-identifier counts
    // and records when the queue becomes empty.
    unique_ptr<BedrockCommand> _dequeue() override;

private:
    // Guards rate limit state (`_identifierCounts`, `_blockedIdentifiers`). Separate from the base
    // class `_queueMutex` because the base mutex is non-recursive and is held while `_dequeue` runs.
    mutex _rateLimitMutex;

    map<string, size_t> _identifierCounts;
    set<string> _blockedIdentifiers;
    atomic<size_t> _maxPerIdentifier{0};
    atomic<uint64_t> _emptyTime{0};
};
