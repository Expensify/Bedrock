#include <BedrockCommandQueue.h>

void BedrockCommandQueue::startTiming(unique_ptr<BedrockCommand>& command)
{
    command->startTiming(BedrockCommand::QUEUE_WORKER);
}

void BedrockCommandQueue::stopTiming(unique_ptr<BedrockCommand>& command)
{
    command->stopTiming(BedrockCommand::QUEUE_WORKER);
}

BedrockCommandQueue::BedrockCommandQueue() :
    _startFunction(startTiming), _endFunction(stopTiming)
{
}

BedrockCommandQueue::BedrockCommandQueue(
    function<void(unique_ptr<BedrockCommand>& item)> startFunction,
    function<void(unique_ptr<BedrockCommand>& item)> endFunction
) : _startFunction(move(startFunction)), _endFunction(move(endFunction))
{
}

void BedrockCommandQueue::clear()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    _queue.clear();
    _lookupByTimeout.clear();
}

bool BedrockCommandQueue::empty()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    return _queue.empty();
}

size_t BedrockCommandQueue::size()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    return _queue.size();
}

unique_ptr<BedrockCommand> BedrockCommandQueue::get(uint64_t waitUS, bool loggingEnabled)
{
    unique_lock<mutex> queueLock(_queueMutex);

    // If there's already work in the queue, just return some.
    try {
        return _dequeue();
    } catch (const out_of_range& e) {
        // Nothing available.
    }

    // Otherwise, we'll wait for some.
    if (waitUS) {
        auto timeout = chrono::steady_clock::now() + chrono::microseconds(waitUS);
        while (true) {
            if (loggingEnabled) {
                SINFO("[performance] Waiting for internal notify or timeout.");
            }
            _queueCondition.wait_until(queueLock, timeout);
            if (loggingEnabled) {
                SINFO("[performance] Notified or timed out, trying to return work.");
            }
            try {
                return _dequeue();
            } catch (const out_of_range& e) {
                // Still nothing available.
            }

            if (chrono::steady_clock::now() > timeout) {
                if (loggingEnabled) {
                    SINFO("[performance] Timed out and there was no work to be done.");
                }
                throw timeout_error();
            }
            if (loggingEnabled) {
                SINFO("[performance] Returned work from the queue, relooping.");
            }
        }
    } else {
        // Wait indefinitely.
        while (true) {
            _queueCondition.wait(queueLock);
            try {
                return _dequeue();
            } catch (const out_of_range& e) {
                // Nothing yet, loop again.
            }
        }
    }
}

void BedrockCommandQueue::push(unique_ptr<BedrockCommand>&& command)
{
    push(move(command), command->scheduledTime);
}

void BedrockCommandQueue::push(unique_ptr<BedrockCommand>&& command, Scheduled time)
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    Timeout timeout = command->timeout();
    _startFunction(command);
    _lookupByTimeout.insert(make_pair(timeout, time));
    _queue.emplace(time, ItemTimeoutPair(move(command), timeout));
    _queueCondition.notify_one();
}

list<unique_ptr<BedrockCommand>> BedrockCommandQueue::getAll()
{
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    list<unique_ptr<BedrockCommand>> items;

    for (auto& entry : _queue) {
        _endFunction(entry.second.item);
        items.emplace_back(move(entry.second.item));
    }

    _queue.clear();
    _lookupByTimeout.clear();

    return items;
}

list<string> BedrockCommandQueue::getRequestMethodLines()
{
    list<string> returnVal;
    SAUTOLOCK(_queueMutex);
    for (auto& entry : _queue) {
        returnVal.push_back(entry.second.item->request.methodLine);
    }
    return returnVal;
}

void BedrockCommandQueue::abandonFutureCommands(int msInFuture)
{
    uint64_t timeLimit = STimeNow() + msInFuture * 1000;

    // Lock around changes to the queue.
    unique_lock<decltype(_queueMutex)> queueLock(_queueMutex);

    auto it = _queue.lower_bound(timeLimit);
    size_t numberToErase = distance(it, _queue.end());
    if (numberToErase) {
        _queue.erase(it, _queue.end());
        SINFO("Erased " << numberToErase << " commands scheduled more than " << msInFuture << "ms in the future.");
    }
}

unique_ptr<BedrockCommand> BedrockCommandQueue::_dequeue()
{
    // NOTE: We don't grab a mutex here on purpose - we use a non-recursive mutex to work with condition_variable, so
    // we need to only lock it once, which we've already done in whichever function is calling this one (since this is
    // private).

    uint64_t now = STimeNow();

    // If anything has timed out, pull that out of the queue, and return that first.
    if (_lookupByTimeout.size()) {
        auto timeoutIt = _lookupByTimeout.begin();
        const Timeout& itemTimeout = timeoutIt->first;
        const Scheduled& itemScheduled = timeoutIt->second;

        if (itemTimeout <= now) {
            // Find all items in the queue scheduled at this particular moment.
            auto matchingItemIterators = _queue.equal_range(itemScheduled);
            size_t itemsChecked = 0;
            for (auto it = matchingItemIterators.first; it != matchingItemIterators.second; it++) {
                itemsChecked++;
                ItemTimeoutPair& thisItemTimeoutPair = it->second;
                if (thisItemTimeoutPair.timeout == itemTimeout) {
                    unique_ptr<BedrockCommand> item = move(thisItemTimeoutPair.item);
                    _queue.erase(it);
                    _lookupByTimeout.erase(timeoutIt);
                    _endFunction(item);
                    return item;
                }
            }
            if (!itemsChecked) {
                SWARN("No items found scheduled at requested time: " << itemScheduled);
            } else {
                SWARN("Checked " << itemsChecked << " items scheduled at " << itemScheduled << " but none had timeout " << itemTimeout);
            }

            // This isn't supposed to be possible.
            SWARN("Timeout (" << itemTimeout << ") before now, but couldn't find an item for it?");
            _lookupByTimeout.erase(timeoutIt);
        }
    }

    // Nothing has timed out — return the first item if it's scheduled at or before now.
    auto itemIt = _queue.begin();
    if (itemIt != _queue.end()) {
        const Scheduled thisItemScheduled = itemIt->first;
        if (thisItemScheduled <= now) {
            ItemTimeoutPair& thisItemTimeoutPair = itemIt->second;
            const Timeout thisItemTimeout = thisItemTimeoutPair.timeout;
            unique_ptr<BedrockCommand> item = move(thisItemTimeoutPair.item);
            _queue.erase(itemIt);

            // Remove the matching entry from the timeout map.
            auto matchingTimeoutIterators = _lookupByTimeout.equal_range(thisItemTimeout);
            for (auto it = matchingTimeoutIterators.first; it != matchingTimeoutIterators.second; it++) {
                if (it->second == thisItemScheduled) {
                    _lookupByTimeout.erase(it);
                    break;
                }
            }

            _endFunction(item);
            return item;
        }
    }

    throw out_of_range("No item found.");
}
