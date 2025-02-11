#include <BedrockCommandQueue.h>

void BedrockCommandQueue::startTiming(unique_ptr<BedrockCommand>& command) {
    command->startTiming(BedrockCommand::QUEUE_WORKER);
}

void BedrockCommandQueue::stopTiming(unique_ptr<BedrockCommand>& command) {
    command->stopTiming(BedrockCommand::QUEUE_WORKER);
}

BedrockCommandQueue::BedrockCommandQueue() :
  SScheduledPriorityQueue<unique_ptr<BedrockCommand>>(function<void(unique_ptr<BedrockCommand>&)>(startTiming), function<void(unique_ptr<BedrockCommand>&)>(stopTiming))
{ }

BedrockCommandQueue::BedrockCommandQueue(
    function<void(unique_ptr<BedrockCommand>& item)> startFunction,
    function<void(unique_ptr<BedrockCommand>& item)> endFunction
) : SScheduledPriorityQueue<unique_ptr<BedrockCommand>>(move(startFunction), move(endFunction))
{ }

list<string> BedrockCommandQueue::getRequestMethodLines() {
    list<string> returnVal;
    SAUTOLOCK(_queueMutex);
    for (auto& queue : _queue) {
        for (auto& entry : queue.second) {
            returnVal.push_back(entry.second.item->request.methodLine);
        }
    }
    return returnVal;
}

void BedrockCommandQueue::abandonFutureCommands(int msInFuture) {
    // We're going to delete every command scehduled after this timestamp.
    uint64_t timeLimit = STimeNow() + msInFuture * 1000;

    // Lock around changes to the queue.
    unique_lock<mutex> queueLock(_queueMutex);

    // We're going to look at each queue by priority. It's possible we'll end up removing *everything* from multiple
    // queues. In that case, we need to remove the queues themselves, so we keep a list of queues to delete when we're
    // done operating on each of them (so that we don't delete them while iterating over them).
    list<typename decltype(_queue)::iterator> toDelete;
    for (typename decltype(_queue)::iterator queueMapIt = _queue.begin(); queueMapIt != _queue.end(); ++queueMapIt) {
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
        _queue.erase(it);
    }
}

void BedrockCommandQueue::push(unique_ptr<BedrockCommand>&& command) {
    SScheduledPriorityQueue<unique_ptr<BedrockCommand>>::push(move(command), command->priority, command->scheduledTime, command->timeout());
}

void BedrockCommandQueue::push(unique_ptr<BedrockCommand>&& command, Scheduled time) {
    SScheduledPriorityQueue<unique_ptr<BedrockCommand>>::push(move(command), command->priority, time, command->timeout());
}
