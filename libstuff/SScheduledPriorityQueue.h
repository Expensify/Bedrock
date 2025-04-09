#pragma once
#include <libstuff/libstuff.h>
#include <condition_variable>

// A scheduled priority queue does the following:
// Enqueues items with a scheduled time, a priority, and a timeout.
// Both the scheduled time and timeout are epoch times in microseconds.
//
// Once queued, a caller can call "get" to retrieve the next item in the queue.
//
// What counts as the next item:
//
// If any item has timed out, it is the timed out item (if multiple items have timed out, it is the one with the oldest
// timeout timestamp. If multiple items have identical timeout timestamps, which if those items is returned is
// unspecified).
//
// If no items have timed out, the items are returned in order of priority, but only if they're scheduled before now.
//
// I.e., if two items are scheduled at the same timestamp, and that timestamp is before now, the one with the higher
// priority is returned.
//
// If two items have the same priority, the one with the older scheduled timestamp is returned.
//
// Items scheduled in the future are never returned (unless they've timed out).
template<typename T>
class SScheduledPriorityQueue {
  public:

    // Typedefs are here for legibility's sake.
    typedef int Priority;
    typedef uint64_t Timeout; 
    typedef uint64_t Scheduled;

    // If nothing becomes available to dequeue while waiting, a timeout_error exception is thrown.
    class timeout_error : exception {
      public:
        const char* what() const noexcept {
            return "timeout";
        }
    };

    // By default, the start and end functions are No-ops.
    SScheduledPriorityQueue(function<void(T& item)> startFunction = [](T& item){},
                            function<void(T& item)> endFunction = [](T& item){})
      : _startFunction(startFunction), _endFunction(endFunction) {};

    // Remove all items from the queue.
    void clear();

    // Clears the queue, returning all the commands.
    list<T> getAll();

    // Returns true if there are no queued commands.
    bool empty();

    // Returns the size of the queue.
    size_t size();

    // Get an item from the queue. Optionally, a timeout can be specified.
    // If timeout is non-zero, a timeout_error exception will be thrown after waitUS microseconds, if no work was
    // available.
    T get(uint64_t waitUS = 0, bool loggingEnabled = false);

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    void push(T&& item, Priority priority, Scheduled scheduled, Timeout timeout);

  protected:

    // Associate the item with it's timeout so that when we dequeue an item to return, we can also remove it's entry
    // in our set of timeouts.
    struct ItemTimeoutPair {
        ItemTimeoutPair(T&& _item, Timeout _timeout) : item(move(_item)), timeout(_timeout) {}
        T item;
        Timeout timeout;
    };

    // Removes an item from the queue and returns it, if a suitable item is available (see the comment at the top of
    // this file for what counts as a suitable item). Throws `out_of_range` otherwise.
    T _dequeue();

    // Synchronization primitives for managing access to the queue.
    mutex _queueMutex;
    condition_variable _queueCondition;

    // The main queue is a map of priorities to the items queued at that priority, sorted by their scheduled time.
    map<Priority, multimap<Scheduled, ItemTimeoutPair>> _queue;

    // A map of timeouts back into the respective priority queue to find the item with the given timeout.
    multimap<Timeout, pair<Priority, Scheduled>> _lookupByTimeout;

    // Functions to call on each item when inserting or removing from the queue.
    function<void(T&)> _startFunction;
    function<void(T&)> _endFunction;
};

template<typename T>
void SScheduledPriorityQueue<T>::clear()  {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    _queue.clear();
    _lookupByTimeout.clear();
}

template<typename T>
bool SScheduledPriorityQueue<T>::empty()  {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    return _queue.empty();
}

template<typename T>
size_t SScheduledPriorityQueue<T>::size()  {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    size_t size = 0;
    for (const auto& queue : _queue) {
        size += queue.second.size();
    }
    return size;
}

template<typename T>
T SScheduledPriorityQueue<T>::get(uint64_t waitUS, bool loggingEnabled) {
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
            // Wait until we hit our timeout, or someone gives us some work.
            _queueCondition.wait_until(queueLock, timeout);
            if (loggingEnabled) {
                SINFO("[performance] Notified or timed out, trying to return work.");
            }
            // If we got any work, return it.
            try {
                return _dequeue();
            } catch (const out_of_range& e) {
                // Still nothing available.
            }

            // Did we go past our timeout? If so, we give up. Otherwise, we awoke spuriously, and will retry.
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

template<typename T>
void SScheduledPriorityQueue<T>::push(T&& item, Priority priority, Scheduled scheduled, Timeout timeout) {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    auto& queue = _queue[priority];
    _startFunction(item);
    _lookupByTimeout.insert(make_pair(timeout, make_pair(priority, scheduled)));
    queue.emplace(scheduled, ItemTimeoutPair(move(item), timeout));
    _queueCondition.notify_one();
    SINFO("Enqueued command with timeout " << timeout);
}

template<typename T>
T SScheduledPriorityQueue<T>::_dequeue() {
    // NOTE: We don't grab a mutex here on purpose - we use a non-recursive mutex to work with condition_variable, so
    // we need to only lock it once, which we've already done in whichever function is calling this one (since this is
    // private).

    // We need to know what time it is, so that we can compare to scheduled times.
    uint64_t now = STimeNow();

    // If anything has timed out, pull that out of the queue, and return that first.
    if (_lookupByTimeout.size()) {

        // Get the first item in the timeout map (they're in order).
        auto timeoutIt = _lookupByTimeout.begin();

        // Convenience names for legibility.
        const Timeout& itemTimeout = timeoutIt->first;
        const Priority& itemPriority = timeoutIt->second.first;
        const Scheduled& itemScheduled = timeoutIt->second.second;

        // Has this timed out? If so, this is the item we'll return (regardless of which priority it had).
        if (itemTimeout <= now) {

            // Find the correct priority queue for this item.
            auto priorityQueueIt = _queue.find(itemPriority);
            if (priorityQueueIt != _queue.end()) {

                // Find all the items in this priority queue scheduled at this particular moment.
                auto matchingItemIterators = priorityQueueIt->second.equal_range(itemScheduled);

                // Iterate across the matching section of items.
                size_t itemsChecked = 0;
                for (auto it = matchingItemIterators.first; it != matchingItemIterators.second; it++) {
                    itemsChecked++;

                    // Convenience names for legibility.
                    ItemTimeoutPair& thisItemTimeoutPair = it->second;

                    // Is this the one that timed out?
                    if (thisItemTimeoutPair.timeout == itemTimeout) {

                        // Yep, this one timed out. Pull it out of the queue.
                        T item = move(thisItemTimeoutPair.item);

                        // Erase this item from the main queue.
                        priorityQueueIt->second.erase(it);

                        // If this priority queue is empty, erase the whole thing.
                        if (priorityQueueIt->second.empty()) {
                            _queue.erase(priorityQueueIt);
                        }

                        // And erase it from the timeout map, too.
                        _lookupByTimeout.erase(timeoutIt);

                        // Call the end function and return the item.
                        _endFunction(item);
                        return item;
                    }
                }
                if (!itemsChecked) {
                    SWARN("No items found scheduled at requested time: " << itemScheduled);
                } else {
                    SWARN("Checked " << itemsChecked << " items scheduled at " << itemScheduled << " but none had timeout " << itemTimeout);
                }
            } else {
                SWARN("No queue found with priority: " << itemPriority);
            }

            // This isn't supposed to be possible.
            SWARN("Timeout (" << itemTimeout << ") before now, but couldn't find a item for it?");
            _lookupByTimeout.erase(timeoutIt);
        }
    }

    // Ok, if we got here nothing has timed out, so we'll just look at each queue, in priority order, to see if any
    // items are ready to return.
    for (auto queueIt = _queue.rbegin(); queueIt != _queue.rend(); ++queueIt) {

        // Record the priority of the queue we're currently looking at.
        Priority queuePriority = queueIt->first;

        // And look at the first item in this particular priority queue.
        auto itemIt = queueIt->second.begin();

        // Convenience names for legibility.
        const Scheduled thisItemScheduled = itemIt->first;
        ItemTimeoutPair& thisItemTimeoutPair = itemIt->second;
        const Timeout thisItemTimeout = thisItemTimeoutPair.timeout;

        // If the item is scheduled before now, we can return it. Otherwise, since these are in scheduled order, there
        // are no usable items in this queue, and we can go on to the next one.
        if (thisItemScheduled <= now) {

            // Pull out the item we want to return.
            T item = move(thisItemTimeoutPair.item);

            // Delete the entry in this queue.
            queueIt->second.erase(itemIt);

            // If the whole queue is empty, delete that too.
            if (queueIt->second.empty()) {
                // The odd syntax in the argument converts a reverse to forward iterator.
                _queue.erase(next(queueIt).base());
            }

            // Remove from the timeout map, as well.
            auto matchingTimeoutIterators = _lookupByTimeout.equal_range(thisItemTimeout);
            for (auto it = matchingTimeoutIterators.first; it != matchingTimeoutIterators.second; it++) {

                // Convenience names for legibility.
                auto& timeoutPair = it->second;
                Priority& thisTimeoutPriority = timeoutPair.first;
                Scheduled& thisTimeoutScheduled = timeoutPair.second;

                // If this timeout entry has the same queue that we're in, and the same scheduled time, we can remove
                // it.
                if (thisTimeoutPriority == queuePriority && thisTimeoutScheduled == thisItemScheduled) {
                    _lookupByTimeout.erase(it);
                    break;
                }

                // We should always break before we get here, some timeout should match.
                SWARN("Did not find a matching timeout (" << thisItemTimeout << ") to remove for command: " << item->request.methodLine);
            }

            // Call the end function and return!
            _endFunction(item);
            return item;
        }
    }

    // No item suitable to return.
    throw out_of_range("No item found.");
}

template<typename T>
list<T> SScheduledPriorityQueue<T>::getAll() {
    lock_guard<decltype(_queueMutex)> lock(_queueMutex);
    list<T> items;

    // Iterate across each map in each queue and pull out all the items.
    for (auto& queue: _queue) {
        for (auto& p : queue.second) {
            items.emplace_back(move(p.second.item));
        }
    }

    // Call the same thing that `clear()` does, but without locking recursively.
    _queue.clear();
    _lookupByTimeout.clear();

    return items;
}
