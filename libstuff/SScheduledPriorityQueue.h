#pragma once
#include <libstuff/libstuff.h>

template<typename T>
class SScheduledPriorityQueue {
  public:
    class timeout_error : exception {
      public:
        const char* what() const noexcept {
            return "timeout";
        }
    };

    SScheduledPriorityQueue(function<void(T& item)> startFunction = [](T& item){}, function<void(T& item)> endFunction = [](T& item){}) : _startFunction(startFunction), _endFunction(endFunction) {};

    // Remove all items from the queue.
    void clear();

    // Returns true if there are no queued commands.
    bool empty();

    // Returns the size of the queue.
    size_t size();

    // Get an item from the queue. Optionally, a timeout can be specified.
    // If timeout is non-zero, an exception will be thrown after timeoutUS microseconds, if no work was available.
    T get(uint64_t timeoutUS = 0);

    // Get an item from the queue, and pass it a counter to be incremented just before dequeuing a found item.
    T getSynchronized(uint64_t timeoutUS, atomic<int>& incrementBeforeDequeue);

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    void push(T&& item, uint64_t executionTimestampUS = 0);

  protected:
    // Removes and returns the first workable command in the queue. A command is workable if it's executeTimestamp is
    // not in the future.
    //
    // "First" means: Of all workable commands, the one in the highest priority queue, with the lowest timestamp of any
    //                command *in that priority queue* - i.e., priority trumps timestamp.
    //
    // This function throws an exception if no workable commands are available.
    T _dequeue(atomic<int>& incrementBeforeDequeue);

    // Synchronization primitives for managing access to the queue.
    mutex _queueMutex;
    condition_variable _queueCondition;

    // The priority queue in which we store commands. This is a map of integer priorities to their respective maps.
    // Each of those maps maps timestamps to commands.
    map<int, multimap<uint64_t, pair<T, uint64_t>>> _commandQueue;

    // This is a map of timeouts to the queue/timestamp we'll need to find the command with this timestamp.
    multimap<uint64_t, pair<int, uint64_t>> _lookupByTimeout;

    function<void(T&)> _startFunction;
    function<void(T&)> _endFunction;
};

template<typename T>
void SScheduledPriorityQueue<T>::clear()  {
    SAUTOLOCK(_queueMutex);
    _commandQueue.clear();
}

template<typename T>
bool SScheduledPriorityQueue<T>::empty()  {
    SAUTOLOCK(_queueMutex);
    return _commandQueue.empty();
}

template<typename T>
size_t SScheduledPriorityQueue<T>::size()  {
    SAUTOLOCK(_queueMutex);
    size_t size = 0;
    for (const auto& queue : _commandQueue) {
        size += queue.second.size();
    }
    return size;
}

template<typename T>
T SScheduledPriorityQueue<T>::get(uint64_t timeoutUS) {
    atomic<int> temp;
    return getSynchronized(timeoutUS, temp);
}

template<typename T>
T SScheduledPriorityQueue<T>::getSynchronized(uint64_t timeoutUS, atomic<int>& incrementBeforeDequeue) {
    unique_lock<mutex> queueLock(_queueMutex);

    // NOTE:
    // Possible future improvement: Say there's work in the queue, but it's not ready yet (i.e., it's scheduled in the
    // future). Someone calls `get(1000000)`, and nothing gets added to the queue during that second (which would wake
    // someone up to process whatever is next, which isn't necessarily the same thing that was added). BUT, some work
    // in the queue comes due during that wait (i.e., it's timestamp is no longer in the future). Currently, we won't
    // wake up here, we'll wait out our full second and force the caller to retry. This is fine for the current
    // (03-2017) use case, where we interrupt every second and only really use scheduling at 1-second granularity.
    //
    // What we could do, is truncate the timeout to not be farther in the future than the next timestamp in the list.

    // If there's already work in the queue, just return some.
    try {
        return _dequeue(incrementBeforeDequeue);
    } catch (const out_of_range& e) {
        // Nothing available.
    }

    // Otherwise, we'll wait for some.
    if (timeoutUS) {
        auto timeout = chrono::steady_clock::now() + chrono::microseconds(timeoutUS);
        while (true) {
            // Wait until we hit our timeout, or someone gives us some work.
            _queueCondition.wait_until(queueLock, timeout);
            
            // If we got any work, return it.
            try {
                return _dequeue(incrementBeforeDequeue);
            } catch (const out_of_range& e) {
                // Still nothing available.
            }

            // Did we go past our timeout? If so, we give up. Otherwise, we awoke spuriously, and will retry.
            if (chrono::steady_clock::now() > timeout) {
                throw timeout_error();
            }
        }
    } else {
        // Wait indefinitely.
        while (true) {
            _queueCondition.wait(queueLock);
            try {
                return _dequeue(incrementBeforeDequeue);
            } catch (const out_of_range& e) {
                // Nothing yet, loop again.
            }
        }
    }
}

template<typename T>
void SScheduledPriorityQueue<T>::push(T&& item, uint64_t executionTimestampUS) {
    SAUTOLOCK(_queueMutex);
    auto& queue = _commandQueue[item.priority];
    _startFunction(item);
    _lookupByTimeout.insert(make_pair(item.timeout(), make_pair(item.priority, executionTimestampUS)));
    queue.emplace(executionTimestampUS, make_pair(move(item), executionTimestampUS));
    _queueCondition.notify_one();
}

template<typename T>
T SScheduledPriorityQueue<T>::_dequeue(atomic<int>& incrementBeforeDequeue) {
    // NOTE: We don't grab a mutex here on purpose - we use a non-recursive mutex to work with condition_variable, so
    // we need to only lock it once, which we've already done in whichever function is calling this one (since this is
    // private).

    // We check to see if a command is going to occur in the future, if so, we won't dequeue it yet.
    uint64_t now = STimeNow();

    // If anything has timed out, pull that out of the queue, and return that first.
    if (_lookupByTimeout.size()) {
        auto timeoutIt = _lookupByTimeout.begin();
        uint64_t timeout = timeoutIt->first;
        if (timeout < now) {
            //this command has timed out.
            int priority = timeoutIt->second.first;
            uint64_t executeTime = timeoutIt->second.second;

            auto individualQueueIt = _commandQueue.find(priority);
            if (individualQueueIt != _commandQueue.end()) {
                auto itPair = individualQueueIt->second.equal_range(executeTime);
                for (auto it = itPair.first; it != itPair.second; it++) {
                    if (it->second.first.timeout() == timeout) {
                        // This is the command that timed out.
                        T command = move(it->second.first);
                        individualQueueIt->second.erase(it);
                        if (individualQueueIt->second.empty()) {
                            _commandQueue.erase(individualQueueIt);
                        }
                        _lookupByTimeout.erase(timeoutIt);
                        _endFunction(command);
                        return command;
                    }
                }
            }

            // We shouldn't have gotten here.
            SWARN("Timeout (" << timeout << ") before now, but couldn't find a command for it?");
            _lookupByTimeout.erase(timeoutIt);
        }
    }

    // Look at each priority queue, starting from the highest priority.
    for (auto queueMapIt = _commandQueue.rbegin(); queueMapIt != _commandQueue.rend(); ++queueMapIt) {
        
        // Look at the first item in the list, this is the one with the lowest timestamp. If this one isn't suitable,
        // none of the others will be, either.
        auto commandMapIt = queueMapIt->second.begin();
        if (commandMapIt->first <= now) {
            // Pull out the command we want to return.
            T command = move(commandMapIt->second.first);
            uint64_t executeTime = commandMapIt->second.second;

            // Make sure we increment this counter before we actually dequeue, so this commands will never be not in
            // the queue and also not counted by the counter.
            incrementBeforeDequeue++;

            // And delete the entry in the queue.
            queueMapIt->second.erase(commandMapIt);

            // If the whole queue is empty, delete that too.
            if (queueMapIt->second.empty()) {
                // The odd syntax in the argument converts a reverse to forward iterator.
                _commandQueue.erase(next(queueMapIt).base());
            }

            // Remove from the timing map, too.
            auto itPair = _lookupByTimeout.equal_range(command.timeout());
            for (auto it = itPair.first; it != itPair.second; it++) {
                if (it->second.first == command.priority && it->second.second == executeTime) {
                    _lookupByTimeout.erase(it);
                    break;
                }
            }

            // Done!
            _endFunction(command);
            return command;
        }
    }

    // No command suitable to process.
    throw out_of_range("No command found.");
}

