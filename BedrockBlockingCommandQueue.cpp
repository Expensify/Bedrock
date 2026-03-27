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

    int maxPerUser = _maxBlockingQueuePerUser.load();
    if (maxPerUser > 0 && !command->blockingIdentifier.empty()) {
        // Clear blocks if the blocking queue has been empty for 30 seconds.
        uint64_t emptyTime = _blockingQueueEmptyTime.load();
        if (emptyTime > 0 && STimeNow() - emptyTime >= 30'000'000) {
            _blockedUsers.clear();
            _blockingQueueUserCounts.clear();
            _blockingQueueEmptyTime.store(0);
        }

        if (_blockedUsers.count(command->blockingIdentifier)) {
            SALERT("Blocking queue rate limit: rejecting '" << command->request.methodLine
                   << "' for identifier '" << command->blockingIdentifier << "'");
            command->response.methodLine = "503 Blocking queue rate limited";
            command->complete = true;
            return true;
        }

        // Only count on first entry from worker threads, not when the blocking commit
        // thread re-queues a command that conflicted again.
        if (!isBlocking) {
            int& count = _blockingQueueUserCounts[command->blockingIdentifier];
            count++;
            if (count >= maxPerUser) {
                _blockedUsers.insert(command->blockingIdentifier);
                SALERT("Blocking queue rate limit: flagging identifier '" << command->blockingIdentifier
                       << "' with " << count << " commands in blocking queue (threshold: " << maxPerUser << ")");
            }
        }
    }

    // Reset empty time since a command is entering the queue.
    _blockingQueueEmptyTime.store(0);

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
    if (command->blockingIdentifier.empty() || _maxBlockingQueuePerUser.load() <= 0) {
        return;
    }
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    auto it = _blockingQueueUserCounts.find(command->blockingIdentifier);
    if (it != _blockingQueueUserCounts.end()) {
        it->second--;
        if (it->second <= 0) {
            _blockingQueueUserCounts.erase(it);
        }
    }
    size_t queueSize = 0;
    for (const auto& q : _queue) {
        queueSize += q.second.size();
    }
    if (queueSize == 0 && _blockingQueueEmptyTime.load() == 0) {
        _blockingQueueEmptyTime.store(STimeNow());
    }
}

void BedrockBlockingCommandQueue::resetRateLimitState()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    _blockingQueueUserCounts.clear();
    _blockedUsers.clear();
    _blockingQueueEmptyTime.store(0);
}

void BedrockBlockingCommandQueue::populateRateLimitStatus(STable& content)
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    content["blockingRateLimitThreshold"] = to_string(_maxBlockingQueuePerUser.load());
    content["blockedUsers"] = to_string(_blockedUsers.size());
    if (!_blockedUsers.empty()) {
        content["blockedUserList"] = SComposeJSONArray(_blockedUsers);
    }
    if (!_blockingQueueUserCounts.empty()) {
        STable countsTable;
        for (const auto& p : _blockingQueueUserCounts) {
            countsTable[p.first] = to_string(p.second);
        }
        content["blockingQueueUserCounts"] = SComposeJSONObject(countsTable);
    }
}

int BedrockBlockingCommandQueue::setMaxPerUser(int value)
{
    int previous = _maxBlockingQueuePerUser.load();
    _maxBlockingQueuePerUser.store(value);
    return previous;
}

void BedrockBlockingCommandQueue::clearBlocks()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    size_t cleared = _blockedUsers.size();
    _blockedUsers.clear();
    _blockingQueueUserCounts.clear();
    _blockingQueueEmptyTime.store(0);
    SINFO("Manually cleared " << cleared << " blocked users.");
}
