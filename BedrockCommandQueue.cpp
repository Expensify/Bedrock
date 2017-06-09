#include <libstuff/libstuff.h>
#include "BedrockCommand.h"
#include "BedrockCommandQueue.h"

void BedrockCommandQueue::clear()  {
    SAUTOLOCK(_queueMutex);
    return _commandQueue.clear();
}

bool BedrockCommandQueue::empty()  {
    SAUTOLOCK(_queueMutex);
    return _commandQueue.empty();
}

size_t BedrockCommandQueue::size()  {
    SAUTOLOCK(_queueMutex);
    size_t size = 0;
    for (const auto& queue : _commandQueue) {
        size += queue.second.size();
    }
    return size;
}

BedrockCommand BedrockCommandQueue::get(uint64_t timeoutUS) {
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
    } catch (...) {
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
                return _dequeue();
            } catch (...) {
                // Still nothing available.
            }

            // Did we go past our timeout? If so, we give up. Otherwise, we awoke spuriously, and will retry.
            if (chrono::steady_clock::now() > timeout) {
                // TODO: Better exception type.
                throw "Timeout";
            }
        }
    } else {
        // Wait indefinitely.
        while (true) {
            _queueCondition.wait(queueLock);
            try {
                return _dequeue();
            } catch (...) {
                // Nothing yet, loop again.
            }
        }
    }
}

list<string> BedrockCommandQueue::getRequestMethodLines() {
    list<string> returnVal;
    SAUTOLOCK(_queueMutex);
    for (auto& queue : _commandQueue) {
        for (auto& entry : queue.second) {
            returnVal.push_back(entry.second.request.methodLine);
        }
    }
    return returnVal;
}

void BedrockCommandQueue::push(BedrockCommand&& item) {
    SAUTOLOCK(_queueMutex);
    auto& queue = _commandQueue[item.priority];
    queue.emplace(item.request.calcU64("commandExecuteTime"), move(item));
    _queueCondition.notify_one();
}

bool BedrockCommandQueue::removeByID(const string& id) {
    SAUTOLOCK(_queueMutex);
    for (auto& queue : _commandQueue) {
        auto it = queue.second.begin();
        while (it != queue.second.end()) {
            if (it->second.id == id) {
                // Found it!
                queue.second.erase(it);
                return true;
            }
        }
    }
    SWARN("Attempted to remove command '" << id << "' but not found.");
    return false;
}

BedrockCommand BedrockCommandQueue::_dequeue() {
    // NOTE: We don't grab a mutex here on purpose - we use a non-recursive mutex to work with condition_variable, so
    // we need to only lock it once, which we've already done in whichever function is calling this one (since this is
    // private).

    // We check to see if a command is going to occur in the future, if so, we won't dequeue it yet.
    uint64_t now = STimeNow();

    // Look at each priority queue, starting from the highest priority.
    for (auto queueMapIt = _commandQueue.rbegin(); queueMapIt != _commandQueue.rend(); ++queueMapIt) {
        
        // Look at the first item in the list, this is the one with the lowest timestamp. If this one isn't suitable,
        // none of the others will be, either.
        auto commandMapIt = queueMapIt->second.begin();
        if (commandMapIt->first <= now) {
            // Pull out the command we want to return.
            BedrockCommand command = move(commandMapIt->second);

            // And delete the entry in the queue.
            queueMapIt->second.erase(commandMapIt);

            // If the whole queue is empty, delete that too.
            if (queueMapIt->second.empty()) {
                // The odd syntax in the argument converts a reverse to forward iterator.
                _commandQueue.erase(next(queueMapIt).base());
            }

            // Done!
            return command;
        }
    }

    // No command suitable to process.
    throw "No command found!";
}
