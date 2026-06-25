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
    const uint64_t maxTimePerIdentifier = _maxTimePerIdentifier.load();
    const bool shouldCheckCount = maxPerIdentifier > 0 && !identifier.empty();
    const bool shouldCheckTime = maxTimePerIdentifier > 0 && !identifier.empty();

    if (shouldCheckCount || shouldCheckTime) {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

        // Clear counts and times if the blocking queue has been empty for 30 seconds.
        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _identifierCounts.clear();
            _identifierTimes.clear();
        }

        if (shouldCheckCount) {
            size_t& count = _identifierCounts[identifier];
            count++;

            if (count > maxPerIdentifier) {
                SINFO("Blocking queue rate limit: rejecting '" << command->request.methodLine
                      << "' for identifier '" << identifier << "' (count=" << count
                      << ", threshold=" << maxPerIdentifier << ")");
                // TODO: enable enforcement after monitoring confirms thresholds are correct in production.
                // count--;
                // STHROW("503 Blocking queue rate limited");
            }
        }

        if (shouldCheckTime) {
            auto it = _identifierTimes.find(identifier);
            const uint64_t timeUS = (it == _identifierTimes.end()) ? 0 : it->second;
            if (timeUS > maxTimePerIdentifier) {
                SINFO("Blocking queue rate limit (time): rejecting '" << command->request.methodLine
                      << "' for identifier '" << identifier << "' (timeMs=" << (timeUS / 1000)
                      << ", thresholdMs=" << (maxTimePerIdentifier / 1000) << ")");
                // TODO: enable enforcement after monitoring confirms thresholds are correct in production.
                // Time-based rollback is not symmetric with count — the rejected command's
                // execution time is not known at push, but the accumulator simply won't grow
                // (the rejected command never runs).
                // STHROW("503 Blocking queue rate limited (time)");
            }
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
        // empty timestamp so the 30-second auto-reset timer isn't lost. Time accumulator was
        // not touched on push, so there's nothing to roll back there.
        _emptyTime.store(previousEmptyTime);
        if (shouldCheckCount) {
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
    _identifierTimes.clear();
    _emptyTime.store(0);
    return size;
}

STable BedrockBlockingCommandQueue::getState()
{
    map<string, size_t> countsCopy;
    map<string, uint64_t> timesCopy;
    {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _identifierCounts.clear();
            _identifierTimes.clear();
        }

        countsCopy = _identifierCounts;
        timesCopy = _identifierTimes;
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

    uint64_t maxTimePerIdentifier = _maxTimePerIdentifier.load();
    size_t blockedTimeCount = 0;
    STable timesTable;
    for (const auto& p : timesCopy) {
        timesTable[p.first] = to_string(p.second / 1000);
        if (p.second > maxTimePerIdentifier) {
            blockedTimeCount++;
        }
    }

    STable content;
    content["blockingRateLimitThreshold"] = to_string(maxPerIdentifier);
    content["blockedIdentifiers"] = to_string(blockedCount);
    if (!countsTable.empty()) {
        content["blockingQueueIdentifierCounts"] = SComposeJSONObject(countsTable);
    }
    content["blockingTimeRateLimitThresholdMs"] = to_string(maxTimePerIdentifier / 1000);
    content["blockedTimeIdentifiers"] = to_string(blockedTimeCount);
    if (!timesTable.empty()) {
        content["blockingQueueIdentifierTimesMs"] = SComposeJSONObject(timesTable);
    }
    return content;
}

size_t BedrockBlockingCommandQueue::setMaxRequestsPerIdentifier(size_t value)
{
    return _maxPerIdentifier.exchange(value);
}

uint64_t BedrockBlockingCommandQueue::setMaxTimePerIdentifier(uint64_t valueUS)
{
    return _maxTimePerIdentifier.exchange(valueUS);
}

void BedrockBlockingCommandQueue::recordExecutionTime(const string& identifier, uint64_t elapsedUS)
{
    if (_maxTimePerIdentifier.load() == 0 || identifier.empty()) {
        return;
    }
    lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);
    _identifierTimes[identifier] += elapsedUS;
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
