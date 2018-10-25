#include <libstuff/libstuff.h>
#include "BedrockCommand.h"
#include "BedrockCommandQueue.h"

void BedrockCommandQueue::clear()  {
    SAUTOLOCK(_queueMutex);
    _commandQueue.clear();
    _timedOut.clear();
}

bool BedrockCommandQueue::empty()  {
    SAUTOLOCK(_queueMutex);
    return _commandQueue.empty() && _timedOut.empty();
}

size_t BedrockCommandQueue::size()  {
    SAUTOLOCK(_queueMutex);
    size_t size = 0;
    for (const auto& queue : _commandQueue) {
        size += queue.second.size();
    }
    size += _timedOut.size();
    return size;
}

BedrockCommand BedrockCommandQueue::get(uint64_t timeoutUS) {
    atomic<int> temp;
    return getSynchronized(timeoutUS, temp);
}

BedrockCommand BedrockCommandQueue::getSynchronized(uint64_t timeoutUS, atomic<int>& incrementBeforeDequeue) {
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
    item.startTiming(BedrockCommand::QUEUE_WORKER);
    _timeouts.insert(item.timeout());
    queue.emplace(item.request.calcU64("commandExecuteTime"), move(item));
    _queueCondition.notify_one();
}

// This function currently never gets called. It's actually completely untested, so if you ever make any changes that
// cause it to actually get called, you'll want to do that testing.
bool BedrockCommandQueue::removeByID(const string& id) {
    SAUTOLOCK(_queueMutex);
    bool retVal = false;
    for (auto queueIt = _commandQueue.begin(); queueIt != _commandQueue.end(); queueIt++) {
        auto& queue = queueIt->second;
        auto it = queue.begin();
        while (it != queue.end()) {
            if (it->second.id == id) {
                // Found it!
                queue.erase(it);
                retVal = true;
                break;
            }
            it++;
        }
        if (retVal) {
            _commandQueue.erase(queueIt);
            break;
        }
    }
    return retVal;
}

void BedrockCommandQueue::abandonFutureCommands(int msInFuture) {
    // We're going to delete every command scehduled after this timestamp.
    uint64_t timeLimit = STimeNow() + msInFuture * 1000;

    // Lock around changes to the queue.
    unique_lock<mutex> queueLock(_queueMutex);

    // We're going to look at each queue by priority. It's possible we'll end up removing *everything* from multiple
    // queues. In that case, we need to remove the queues themselves, so we keep a list of queues to delete when we're
    // done operating on each of them (so that we don't delete them while iterating over them).
    list<decltype(_commandQueue)::iterator> toDelete;
    for (decltype(_commandQueue)::iterator queueMapIt = _commandQueue.begin(); queueMapIt != _commandQueue.end(); ++queueMapIt) {
        // Starting from the first item, skip any items that have a valid scheduled time.
        auto commandMapIt = queueMapIt->second.begin();
        while (commandMapIt != queueMapIt->second.end() && commandMapIt->first < timeLimit) {
            commandMapIt++;
        }

        // Whatever's left in the queue is scheduled in the future and can be erased.
        size_t numberToErase = distance(commandMapIt, queueMapIt->second.end());
        if (numberToErase) {
            queueMapIt->second.erase(commandMapIt, queueMapIt->second.end());
        }

        // If the whole queue is empty, save it for deletion.
        if (queueMapIt->second.empty()) {
            toDelete.push_back(queueMapIt);
        }

        // If we deleted any commands, log that.
        if (numberToErase) {
            SINFO("Erased " << numberToErase << " commands scheduled more than " << msInFuture << "ms in the future.");
        }
    }

    // Delete any empty queues.
    for (auto& it : toDelete) {
        _commandQueue.erase(it);
    }
}

BedrockCommand BedrockCommandQueue::_dequeue(atomic<int>& incrementBeforeDequeue) {
    // NOTE: We don't grab a mutex here on purpose - we use a non-recursive mutex to work with condition_variable, so
    // we need to only lock it once, which we've already done in whichever function is calling this one (since this is
    // private).

    // We check to see if a command is going to occur in the future, if so, we won't dequeue it yet.
    uint64_t now = STimeNow();

    // Before we look at out main queue, check for timed out commands.
    // Look at the front of _timeouts. If it's before now, we need to move commands to the _timedOut queue.
    // We do this by walking all the commands, moving them to _timedOut, and removing their timeout() value from
    // _timeouts.
    //
    // As an optimization, we only start timing things out when they've actually timed out over a 0.1s ago. This
    // means that we only look at the set of _timeouts when at least one command has been timed out for 100ms, but we
    // move everything that's timed out to that list, meaning that we'll have to wait at least 100ms until the first
    // item in _timeouts can time out and cause us to walk the list again. The worry is we'd end up walking the list on
    // every single command when a lot of commands were on the verge of timing out in a row.
    if (_timeouts.size()) {
        uint64_t firstTimeout = *(_timeouts.begin());
        if (firstTimeout <= (now - 100'000)) {
            int64_t countOfInspected = 0;
            int64_t countOfRemoved = 0;
            // Walk the list of queues.
            for (auto queueMapIt = _commandQueue.begin(); queueMapIt != _commandQueue.end(); ++queueMapIt) {
                // Walk the commands in this queue.
                auto& queue =  queueMapIt->second;
                for (auto queueIt = queue.begin(); queueIt != queue.end(); ++queueIt) {
                    countOfInspected++;
                    if (queueIt->second.timeout() < now) {
                        // This command has timed out, remove it's entry from the _timeouts map.
                        _timeouts.erase(queueIt->second.timeout());

                        // And move the command to the _timedOut, erasing it from this queue.
                        _timedOut.push_back(move(queueIt->second));
                        queueIt = queue.erase(queueIt);
                        countOfRemoved++;
                    }
                }

                // If we deleted everything in the sub-queue, remove the whole thing.
                if (queue.empty()) {
                    queueMapIt = _commandQueue.erase(queueMapIt);
                }
            }
            uint64_t finished = STimeNow();
            SINFO("Inspected " << countOfInspected << " to remove " << countOfRemoved << " timed out commands in " << (finished - now) << "us.");
        }
    }

    // Now, if there are any commands in the timed out list, we can return the first one.
    if (_timedOut.size()) {
        BedrockCommand command = move(_timedOut.front());
        _timedOut.pop_front();
        return command;
    }

    // Look at each priority queue, starting from the highest priority.
    for (auto queueMapIt = _commandQueue.rbegin(); queueMapIt != _commandQueue.rend(); ++queueMapIt) {
        
        // Look at the first item in the list, this is the one with the lowest timestamp. If this one isn't suitable,
        // none of the others will be, either.
        auto commandMapIt = queueMapIt->second.begin();
        if (commandMapIt->first <= now) {
            // Pull out the command we want to return.
            BedrockCommand command = move(commandMapIt->second);
            _timeouts.erase(command.timeout()); // TODO: This can be wrong if another command had this timeout.

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

            // Done!
            command.stopTiming(BedrockCommand::QUEUE_WORKER);
            return command;
        }
    }

    // No command suitable to process.
    throw out_of_range("No command found.");
}
