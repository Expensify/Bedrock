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
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);

    size_t maxPerIdentifier = _maxPerIdentifier.load();
    if (maxPerIdentifier > 0 && !command->blockingIdentifier.empty()) {
        // Clear blocks if the blocking queue has been empty for 30 seconds.
        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _blockedIdentifiers.clear();
            _identifierCounts.clear();
        }

        if (_blockedIdentifiers.count(command->blockingIdentifier)) {
            SINFO("Blocking queue rate limit: rejecting '" << command->request.methodLine
                  << "' for identifier '" << command->blockingIdentifier << "'");
            STHROW("503 Blocking queue rate limited");
        }

        size_t& count = _identifierCounts[command->blockingIdentifier];
        count++;
        if (count > maxPerIdentifier) {
            _blockedIdentifiers.insert(command->blockingIdentifier);
            SWARN("Blocking queue rate limit: blocking identifier '" << command->blockingIdentifier
                  << "' with " << count << " commands in blocking queue (threshold: " << maxPerIdentifier << ")");
        }
    }

    // A command is entering the queue, so it is no longer empty. Clear the empty timestamp so
    // the 30-second auto-reset window doesn't fire until the queue drains again.
    _emptyTime.store(0);

    // Delegate to parent; _queueMutex is recursive so this re-acquisition is safe.
    BedrockCommandQueue::push(move(command));
}

unique_ptr<BedrockCommand> BedrockBlockingCommandQueue::_dequeue()
{
    // Called by get() with _queueMutex held, so access to rate limit state is safe without locking.
    auto command = SScheduledPriorityQueue<unique_ptr<BedrockCommand>>::_dequeue();

    // Decrement rate limit count when a command leaves the queue.
    if (!command->blockingIdentifier.empty() && _maxPerIdentifier.load() > 0) {
        auto it = _identifierCounts.find(command->blockingIdentifier);
        if (it != _identifierCounts.end()) {
            if (it->second <= 1) {
                _identifierCounts.erase(it);
            } else {
                it->second--;
            }
        }
    }

    // Track when the queue becomes empty for auto-clearing blocks.
    size_t queueSize = 0;
    for (const auto& q : _queue) {
        queueSize += q.second.size();
    }
    if (queueSize == 0 && _emptyTime.load() == 0) {
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
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    size_t cleared = _blockedIdentifiers.size();
    _identifierCounts.clear();
    _blockedIdentifiers.clear();
    _emptyTime.store(0);
    return cleared;
}

STable BedrockBlockingCommandQueue::getState()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
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
