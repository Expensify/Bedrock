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
    const string identifier = command->blockingIdentifier;
    const size_t maxPerIdentifier = _maxPerIdentifier.load();
    const bool checking = maxPerIdentifier > 0 && !identifier.empty();

    if (checking) {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

        // Clear blocks if the blocking queue has been empty for 30 seconds.
        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _blockedIdentifiers.clear();
            _identifierCounts.clear();
        }

        if (_blockedIdentifiers.count(identifier)) {
            SINFO("Blocking queue rate limit: rejecting '" << command->request.methodLine
                  << "' for identifier '" << identifier << "'");
            STHROW("503 Blocking queue rate limited");
        }

        size_t& count = _identifierCounts[identifier];
        count++;
        if (count > maxPerIdentifier) {
            _blockedIdentifiers.insert(identifier);
            SWARN("Blocking queue rate limit: blocking identifier '" << identifier
                  << "' with " << count << " commands in blocking queue (threshold: " << maxPerIdentifier << ")");
        }
    }

    // A command is entering the queue, so it is no longer empty. Clear the empty timestamp so
    // the 30-second auto-reset window doesn't fire until the queue drains again.
    _emptyTime.store(0);

    try {
        // Base class acquires its own (non-recursive) `_queueMutex`.
        BedrockCommandQueue::push(move(command));
    } catch (...) {
        // Roll back the increment so a base-class enqueue failure can't permanently inflate counts
        // and falsely block this identifier.
        if (checking) {
            lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);
            auto it = _identifierCounts.find(identifier);
            if (it != _identifierCounts.end()) {
                if (it->second <= 1) {
                    _identifierCounts.erase(it);
                } else {
                    it->second--;
                }
            }
        }
        throw;
    }
}

unique_ptr<BedrockCommand> BedrockBlockingCommandQueue::_dequeue()
{
    // Called by `BedrockCommandQueue::get()` with the base `_queueMutex` held. Inspect `_queue` directly here;
    // calling any base method that reacquires `_queueMutex` would deadlock.
    auto command = BedrockCommandQueue::_dequeue();

    // Decrement rate limit count when a command leaves the queue.
    if (!command->blockingIdentifier.empty() && _maxPerIdentifier.load() > 0) {
        lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);
        auto it = _identifierCounts.find(command->blockingIdentifier);
        if (it != _identifierCounts.end()) {
            if (it->second <= 1) {
                _identifierCounts.erase(it);
            } else {
                it->second--;
            }
        }
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
    size_t cleared = _blockedIdentifiers.size();
    _identifierCounts.clear();
    _blockedIdentifiers.clear();
    _emptyTime.store(0);
    return cleared;
}

STable BedrockBlockingCommandQueue::getState()
{
    lock_guard<decltype(_rateLimitMutex)> lock(_rateLimitMutex);

    uint64_t emptyTime = _emptyTime.load();
    if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
        _blockedIdentifiers.clear();
        _identifierCounts.clear();
    }

    STable content;
    content["blockingRateLimitThreshold"] = to_string(_maxPerIdentifier.load());
    content["blockedIdentifiers"] = to_string(_blockedIdentifiers.size());
    if (!_blockedIdentifiers.empty()) {
        content["blockedIdentifierList"] = SComposeJSONArray(_blockedIdentifiers);
    }
    if (!_identifierCounts.empty()) {
        STable countsTable;
        for (const auto& p : _identifierCounts) {
            countsTable[p.first] = to_string(p.second);
        }
        content["blockingQueueIdentifierCounts"] = SComposeJSONObject(countsTable);
    }
    return content;
}

size_t BedrockBlockingCommandQueue::setMaxPerIdentifier(size_t value)
{
    return _maxPerIdentifier.exchange(value);
}
