#include <BedrockBlockingCommandQueue.h>

void BedrockBlockingCommandQueue::startTiming(unique_ptr<BedrockCommand>& command)
{
    command->startTiming(BedrockCommand::QUEUE_BLOCKING);
}

void BedrockBlockingCommandQueue::stopTiming(unique_ptr<BedrockCommand>& command)
{
    command->stopTiming(BedrockCommand::QUEUE_BLOCKING);
}

BedrockBlockingCommandQueue::BedrockBlockingCommandQueue() :
    BedrockCommandQueue(function<void(unique_ptr<BedrockCommand>&)>(startTiming),
                        function<void(unique_ptr<BedrockCommand>&)>(stopTiming))
{
}

void BedrockBlockingCommandQueue::push(unique_ptr<BedrockCommand>&& command)
{
    const string identifier = command->blockingQueueRateLimitIdentifier;
    const size_t maxPerIdentifier = _maxPerIdentifier.load();
    const bool shouldCheck = maxPerIdentifier > 0 && !identifier.empty();

    if (shouldCheck) {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

        // Clear counts if the blocking queue has been empty for 30 seconds.
        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _identifierCounts.clear();
        }

        size_t& count = _identifierCounts[identifier];

        if (count > maxPerIdentifier) {
            SINFO("Blocking queue rate limit: rejecting '" << command->request.methodLine
                  << "' for identifier '" << identifier << "'");
            // TODO: enable enforcement after monitoring confirms thresholds are correct in production.
            // STHROW("503 Blocking queue rate limited");
        }

        count++;
        if (count > maxPerIdentifier) {
            SWARN("Blocking queue rate limit: blocking identifier '" << identifier
                  << "' with " << count << " commands in blocking queue (threshold: " << maxPerIdentifier << ")");
        }
    }

    // A command is entering the queue, so it is no longer empty. Clear the empty timestamp so
    // the 30-second auto-reset window doesn't fire until the queue drains again.
    uint64_t previousEmptyTime = _emptyTime.exchange(0);

    try {
        // Base class acquires its own (non-recursive) `_queueMutex`.
        BedrockCommandQueue::push(move(command));
    } catch (...) {
        // The command never entered the queue. Roll back the count increment and restore the
        // empty timestamp so the 30-second auto-reset timer isn't lost.
        _emptyTime.store(previousEmptyTime);
        if (shouldCheck) {
            lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);
            _decrementIdentifierCount(identifier);
        }
        throw;
    }
}

/**
 * Dequeues command and inspects _queue to update rate limit counts and _emptyTime
 * Called by `BedrockCommandQueue::get()` with the base `_queueMutex` held. Calling any base method that reacquires `_queueMutex` would deadlock.
 */
unique_ptr<BedrockCommand> BedrockBlockingCommandQueue::_dequeue()
{
    auto command = BedrockCommandQueue::_dequeue();

    // Decrement rate limit count when a command leaves the queue.
    if (!command->blockingQueueRateLimitIdentifier.empty() && _maxPerIdentifier.load() > 0) {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);
        _decrementIdentifierCount(command->blockingQueueRateLimitIdentifier);
    }

    if (_queue.empty() && _emptyTime.load() == 0) {
        _emptyTime.store(STimeNow());
    }

    return command;
}

void BedrockBlockingCommandQueue::clear()
{
    clearRateLimits();
    BedrockCommandQueue::clear();
}

size_t BedrockBlockingCommandQueue::clearRateLimits()
{
    lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);
    size_t size = _identifierCounts.size();
    _identifierCounts.clear();
    _emptyTime.store(0);
    return size;
}

STable BedrockBlockingCommandQueue::getState()
{
    map<string, size_t> countsCopy;
    {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _identifierCounts.clear();
        }

        countsCopy = _identifierCounts;
    }

    size_t maxPerIdentifier = _maxPerIdentifier.load();
    size_t blockedCount = 0;
    STable countsTable;
    for (const auto& p : countsCopy) {
        countsTable[p.first] = to_string(p.second);
        if (p.second > maxPerIdentifier) {
            blockedCount++;
        }
    }

    STable content;
    content["blockingRateLimitThreshold"] = to_string(maxPerIdentifier);
    content["blockedIdentifiers"] = to_string(blockedCount);
    if (!countsTable.empty()) {
        content["blockingQueueIdentifierCounts"] = SComposeJSONObject(countsTable);
    }
    return content;
}

size_t BedrockBlockingCommandQueue::setMaxRequestsPerIdentifier(size_t value)
{
    return _maxPerIdentifier.exchange(value);
}

void BedrockBlockingCommandQueue::_decrementIdentifierCount(const string& identifier)
{
    auto it = _identifierCounts.find(identifier);
    if (it != _identifierCounts.end()) {
        if (it->second <= 1) {
            _identifierCounts.erase(it);
        } else {
            it->second--;
        }
    }
}
