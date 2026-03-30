#pragma once
#include "BedrockCommandQueue.h"

class BedrockCommand;

class BedrockBlockingCommandQueue : public BedrockCommandQueue {
public:
    BedrockBlockingCommandQueue();

    // Functions to start and stop timing on the commands when they're inserted/removed from the queue.
    static void startTiming(unique_ptr<BedrockCommand>& command);
    static void stopTiming(unique_ptr<BedrockCommand>& command);

    // Check per-identifier rate limit before pushing a command to the blocking queue.
    // Returns true if the command was rejected (503 set on response), caller should reply and return.
    // Returns false if the command was pushed onto the queue (command is moved, caller should not use it).
    bool checkRateLimitAndPush(unique_ptr<BedrockCommand>& command);

    // Decrement the rate limit count for a command leaving the blocking queue.
    void decrementCount(const unique_ptr<BedrockCommand>& command);

    // Clear all rate limiting state.
    void resetRateLimitState();

    // Populate the given table with rate limiting status info for the Status command.
    void populateRateLimitStatus(STable& content);

    // Set the max commands per identifier threshold. Returns the previous value.
    size_t setMaxPerIdentifier(size_t value);

    // Clear all blocked identifiers and counts.
    void clearBlocks();

private:
    map<string, int> _identifierCounts;
    set<string> _blockedIdentifiers;
    atomic<size_t> _maxPerIdentifier{0};
    atomic<uint64_t> _emptyTime{0};
};
