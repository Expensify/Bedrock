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
    BedrockCommandQueue(function<void(unique_ptr<BedrockCommand>&)>(startTiming), function<void(unique_ptr<BedrockCommand>&)>(stopTiming))
{
}

bool BedrockBlockingCommandQueue::checkRateLimitAndPush(unique_ptr<BedrockCommand>& command, bool isBlocking)
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);

    size_t maxPerIdentifier = _maxPerIdentifier.load();
    if (maxPerIdentifier > 0 && !command->blockingIdentifier.empty()) {
        // Clear blocks if the blocking queue has been empty for 30 seconds.
        uint64_t emptyTime = _emptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _blockedIdentifiers.clear();
            _identifierCounts.clear();
            _emptyTime.store(0);
        }

        if (_blockedIdentifiers.count(command->blockingIdentifier)) {
            SALERT("Blocking queue rate limit: rejecting '" << command->request.methodLine
                   << "' for identifier '" << command->blockingIdentifier << "'");
            command->response.methodLine = "503 Blocking queue rate limited";
            command->complete = true;
            return true;
        }

        // Only count on first entry from worker threads, not when the blocking commit
        // thread re-queues a command that conflicted again.
        if (!isBlocking) {
            int& count = _identifierCounts[command->blockingIdentifier];
            count++;
            if (count >= (int)maxPerIdentifier) {
                _blockedIdentifiers.insert(command->blockingIdentifier);
                SALERT("Blocking queue rate limit: flagging identifier '" << command->blockingIdentifier
                       << "' with " << count << " commands in blocking queue (threshold: " << maxPerIdentifier << ")");
            }
        }
    }

    // Reset empty time since a command is entering the queue.
    _emptyTime.store(0);

    // Inline the push to avoid double-locking _queueMutex.
    auto priority = command->priority;
    auto scheduledTime = command->scheduledTime;
    auto timeout = command->timeout();
    _startFunction(command);
    _lookupByTimeout.insert(make_pair(timeout, make_pair(priority, scheduledTime)));
    _queue[priority].emplace(scheduledTime, ItemTimeoutPair(move(command), timeout));
    _queueCondition.notify_one();
    return false;
}

void BedrockBlockingCommandQueue::decrementCount(const unique_ptr<BedrockCommand>& command)
{
    if (command->blockingIdentifier.empty() || _maxPerIdentifier.load() <= 0) {
        return;
    }
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    auto it = _identifierCounts.find(command->blockingIdentifier);
    if (it != _identifierCounts.end()) {
        it->second--;
        if (it->second <= 0) {
            _identifierCounts.erase(it);
        }
    }
    size_t queueSize = 0;
    for (const auto& q : _queue) {
        queueSize += q.second.size();
    }
    if (queueSize == 0 && _emptyTime.load() == 0) {
        _emptyTime.store(STimeNow());
    }
}

void BedrockBlockingCommandQueue::resetRateLimitState()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    _identifierCounts.clear();
    _blockedIdentifiers.clear();
    _emptyTime.store(0);
}

void BedrockBlockingCommandQueue::populateRateLimitStatus(STable& content)
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
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
}

size_t BedrockBlockingCommandQueue::setMaxPerIdentifier(size_t value)
{
    size_t previous = _maxPerIdentifier.load();
    _maxPerIdentifier.store(value);
    return previous;
}

void BedrockBlockingCommandQueue::clearBlocks()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    size_t cleared = _blockedIdentifiers.size();
    _blockedIdentifiers.clear();
    _identifierCounts.clear();
    _emptyTime.store(0);
    SINFO("Manually cleared " << cleared << " blocked identifiers.");
}
