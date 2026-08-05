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

    // Reject before enqueuing if the identifier is over the allowed time spent in the blocking queue.
    if (isIdentifierOverTimeLimit(identifier, command->request.methodLine)) {
        STHROW("503 Blocking queue rate limited (time)");
    }

    // A command is entering the queue, so it is no longer empty. Clear the empty timestamp so
    // the 30-second auto-reset window doesn't fire until the queue drains again.
    uint64_t previousEmptyTime = _emptyTime.exchange(0);

    try {
        // Base class acquires its own (non-recursive) `_queueMutex`.
        BedrockCommandQueue::push(move(command));
    } catch (...) {
        // The command never entered the queue. Restore the empty timestamp so the
        // 30-second auto-reset timer isn't lost.
        _emptyTime.store(previousEmptyTime);
        throw;
    }
}

/**
 * Dequeues command and inspects _queue to update _emptyTime, and rejects the command if its identifier is over the time limit.
 * Called by `BedrockCommandQueue::get()` with the base `_queueMutex` held. Calling any base method that reacquires `_queueMutex` would deadlock.
 */
unique_ptr<BedrockCommand> BedrockBlockingCommandQueue::_dequeue()
{
    auto command = BedrockCommandQueue::_dequeue();

    const string blockingIdentifier = command->blockingQueueRateLimitIdentifier;

    if (_queue.empty() && _emptyTime.load() == 0) {
        _emptyTime.store(STimeNow());
    }

    // If this command has a blocking queue identifier, check if it's over the time limit. If so, fill the response methodLine with 503
    // and set the command as completed. By doing so, we will skip processing the command in `BedrockServer::runCommand`.
    if (!blockingIdentifier.empty() && isIdentifierOverTimeLimit(blockingIdentifier, command->request.methodLine)) {
        command->response.methodLine = "503 Blocking queue rate limited (time)";
        command->complete = true;
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
    size_t size = _identifierTimes.size();
    _identifierTimes.clear();
    _emptyTime.store(0);
    return size;
}

STable BedrockBlockingCommandQueue::getState()
{
    map<string, uint64_t> timesCopy;
    {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _identifierTimes.clear();
        }

        timesCopy = _identifierTimes;
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
    content["blockingTimeRateLimitThresholdMs"] = to_string(maxTimePerIdentifier / 1000);
    content["blockedTimeIdentifiers"] = to_string(blockedTimeCount);
    if (!timesTable.empty()) {
        content["blockingQueueIdentifierTimesMs"] = SComposeJSONObject(timesTable);
    }
    return content;
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

bool BedrockBlockingCommandQueue::isIdentifierOverTimeLimit(const string& identifier, const string& methodLine)
{
    const uint64_t maxTimePerIdentifier = _maxTimePerIdentifier.load();
    if (maxTimePerIdentifier == 0 || identifier.empty()) {
        return false;
    }

    lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

    // Clear accumulated times if the blocking queue has been empty for 30 seconds.
    uint64_t emptyTime = _emptyTime.load();
    if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
        _identifierTimes.clear();
    }

    auto it = _identifierTimes.find(identifier);
    const uint64_t timeUS = (it == _identifierTimes.end()) ? 0 : it->second;

    if (timeUS > maxTimePerIdentifier) {
        SINFO("Blocking queue rate limit (time), rejecting", {
            {"command", methodLine},
            {"identifier", identifier},
            {"timeMS", to_string(timeUS / 1000)},
            {"thresholdMS", to_string(maxTimePerIdentifier / 1000)}
        });
        return true;
    }

    if (timeUS > _maxTimePerIdentifierToLog.load()) {
        SINFO("Blocking queue rate limit (time), logging", {
            {"command", methodLine},
            {"identifier", identifier},
            {"timeMS", to_string(timeUS / 1000)},
            {"thresholdMS", to_string(maxTimePerIdentifier / 1000)}
        });
    }

    return false;
}
