/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    BedrockCommandQueue.h
 * Path:    BedrockCommandQueue.h
 * Pair:    BedrockCommandQueue.cpp
 *
 * INTENT
 *   A thread-safe, time-ordered queue of BedrockCommands keyed by both
 *   scheduled time and timeout, from which worker threads block-and-wait
 *   (get()) for the next command that is either already timed out or due
 *   to run.
 *
 * OBJECTS
 *   BedrockCommandQueue                  - class; push/get/_dequeue, the
 *     timed-out-vs-scheduled ordering rule, and empty/size/clear/getAll
 *     queries over the queue.
 *   BedrockCommandQueue::timeout_error   - nested exception class; thrown
 *     by get() when its wait window elapses with nothing available.
 *   BedrockCommandQueue::ItemTimeoutPair - protected nested struct; pairs a
 *     queued command with its timeout for the parallel `_lookupByTimeout`
 *     index.
 *   BedrockCommandQueue::Timeout / Scheduled - typedefs for uint64_t
 *     epoch-microsecond timestamps, distinguishing the two orderings.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Timeout and Scheduled are both bare typedefs for uint64_t, with nothing
 *   at the type level to stop one being passed where the other is expected
 *   (e.g. to push(command, time)) - the exact "two distinct concepts both
 *   typedef'd to the same primitive" confusion this section watches for.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <condition_variable>
#include <libstuff/libstuff.h>
#include "BedrockCommand.h"

// A scheduled queue of BedrockCommands.
//
// Items are enqueued with a scheduled time and a timeout (both epoch microseconds). `get` returns the next available
// item:
//   - If any item has timed out, the one with the oldest timeout is returned (regardless of scheduled time).
//   - Otherwise, the item with the oldest scheduled time is returned, but only if that time is at or before now.
//   - Items scheduled in the future are never returned (unless they've timed out).
class BedrockCommandQueue {
public:
    typedef uint64_t Timeout;
    typedef uint64_t Scheduled;

    // Thrown by `get` if no work becomes available within the wait window.
    class timeout_error : exception {
public:
        const char* what() const noexcept
        {
            return "timeout";
        }
    };

    BedrockCommandQueue();
    BedrockCommandQueue(
        function<void(unique_ptr<BedrockCommand>& item)> startFunction,
        function<void(unique_ptr<BedrockCommand>& item)> endFunction
    );
    virtual ~BedrockCommandQueue() = default;

    // Functions to start and stop timing on the commands when they're inserted/removed from the queue.
    static void startTiming(unique_ptr<BedrockCommand>& command);
    static void stopTiming(unique_ptr<BedrockCommand>& command);

    // Remove all items from the queue.
    void clear();

    // Clears the queue, returning all the commands.
    list<unique_ptr<BedrockCommand>> getAll();

    // Returns true if there are no queued commands.
    bool empty();

    // Returns the size of the queue.
    size_t size();

    // Get an item from the queue. If `waitUS` is non-zero, throws `timeout_error` after that many microseconds if
    // nothing became available.
    unique_ptr<BedrockCommand> get(uint64_t waitUS = 0, bool loggingEnabled = false);

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    virtual void push(unique_ptr<BedrockCommand>&& command);

    // Add an item to the queue with a custom scheduled time. The queue takes ownership of the item.
    void push(unique_ptr<BedrockCommand>&& command, Scheduled time);

    // Returns a list of all the method lines for all the requests currently queued.
    list<string> getRequestMethodLines();

    // Discards all commands scheduled more than msInFuture milliseconds after right now.
    void abandonFutureCommands(int msInFuture);

protected:
    // Associate the item with its timeout so that when we dequeue an item to return, we can also remove its entry in
    // our set of timeouts.
    struct ItemTimeoutPair
    {
        ItemTimeoutPair(unique_ptr<BedrockCommand>&& _item, Timeout _timeout) : item(move(_item)), timeout(_timeout)
        {
        }

        unique_ptr<BedrockCommand> item;
        Timeout timeout;
    };

    // Removes an item from the queue and returns it, if a suitable item is available. Throws `out_of_range` otherwise.
    // Called with `_queueMutex` held by the caller.
    virtual unique_ptr<BedrockCommand> _dequeue();

    // Items sorted by scheduled time. Exposed to subclasses so overrides of `_dequeue` can inspect emptiness while
    // `_queueMutex` is held.
    multimap<Scheduled, ItemTimeoutPair> _queue;

private:
    // Synchronization primitives for managing access to the queue.
    mutex _queueMutex;
    condition_variable _queueCondition;

    // A map of timeouts back into the main queue to find the item with the given timeout.
    multimap<Timeout, Scheduled> _lookupByTimeout;

    // Functions to call on each item when inserting or removing from the queue.
    function<void(unique_ptr<BedrockCommand>&)> _startFunction;
    function<void(unique_ptr<BedrockCommand>&)> _endFunction;
};
