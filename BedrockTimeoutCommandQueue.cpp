#include <BedrockTimeoutCommandQueue.h>

const BedrockCommandPtr& BedrockTimeoutCommandQueue::front() const {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    if (_queue.empty()) {
        throw out_of_range("No commands");
    }

    // has anything timed out?
    if (_timeoutMap.begin()->first < STimeNow()) {
        // first item has timed out, that's the effective front.
        return *(_timeoutMap.begin()->second);
    }
    return _queue.front();
}

void BedrockTimeoutCommandQueue::push(BedrockCommandPtr&& rhs) {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);

    // Add to the queue and timeout map.
    _queue.push_back(move(rhs));
    _queue.back()->startTiming(BedrockCommand::QUEUE_SYNC);

    // This is past-the-end, so we decrement it to point to the last element.
    auto lastIt = _queue.end();
    lastIt--;
    _timeoutMap.insert(make_pair((*lastIt)->timeout(), lastIt));

    // Write arbitrary buffer to the pipe so any subscribers will be awoken.
    // **NOTE: 1 byte so write is atomic.
    SASSERT(write(_pipeFD[1], "A", 1));
}

BedrockCommandPtr BedrockTimeoutCommandQueue::pop() {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    if (_queue.empty()) {
        throw out_of_range("No commands");
    }
    if (_timeoutMap.begin()->first < STimeNow()) {
        BedrockCommandPtr item = move(*(_timeoutMap.begin()->second));
        _queue.erase(_timeoutMap.begin()->second);
        _timeoutMap.erase(_timeoutMap.begin());
        item->stopTiming(BedrockCommand::QUEUE_SYNC);
        return item;
    }

    // We need to remove the reference in the timeout map for this item as well.
    auto firstCommandIt = _queue.begin();
    auto itPair = _timeoutMap.equal_range((*firstCommandIt)->timeout());
    for (auto it = itPair.first; it != itPair.second; it++) {
        if (it->second == firstCommandIt) {
            // This one points at this command, remove it.
            _timeoutMap.erase(it);
            break;
        }
    }
    BedrockCommandPtr item = move(*firstCommandIt);
    item->stopTiming(BedrockCommand::QUEUE_SYNC);
    _queue.pop_front();
    return item;
}
