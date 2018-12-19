#include <BedrockCommandQueue.h>

void BedrockCommandQueue::startTiming(BedrockCommand& item) {
    item.startTiming(BedrockCommand::QUEUE_WORKER);
}

void BedrockCommandQueue::stopTiming(BedrockCommand& item) {
    item.stopTiming(BedrockCommand::QUEUE_WORKER);
}

BedrockCommandQueue::BedrockCommandQueue() :
  SQueue<BedrockCommand>(function<void(BedrockCommand&)>(startTiming), function<void(BedrockCommand&)>(stopTiming))
{ }

list<string> BedrockCommandQueue::getRequestMethodLines() {
    list<string> returnVal;
    SAUTOLOCK(_queueMutex);
    for (auto& queue : _commandQueue) {
        for (auto& entry : queue.second) {
            returnVal.push_back(entry.second.first.request.methodLine);
        }
    }
    return returnVal;
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
            if (it->second.first.id == id) {
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
    list<typename decltype(_commandQueue)::iterator> toDelete;
    for (typename decltype(_commandQueue)::iterator queueMapIt = _commandQueue.begin(); queueMapIt != _commandQueue.end(); ++queueMapIt) {
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

void BedrockCommandQueue::push(BedrockCommand&& item) {
    uint64_t exectionTime = item.request.calcU64("commandExecuteTime");
    SQueue<BedrockCommand>::push(move(item), exectionTime);
}
