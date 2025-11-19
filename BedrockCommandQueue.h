#pragma once
#include <libstuff/libstuff.h>
#include <libstuff/SScheduledPriorityQueue.h>
#include "BedrockCommand.h"

class BedrockCommandQueue : public SScheduledPriorityQueue<unique_ptr<BedrockCommand>> {
public:
    BedrockCommandQueue();
    BedrockCommandQueue(
        function<void(unique_ptr<BedrockCommand>& item)> startFunction,
        function<void(unique_ptr<BedrockCommand>& item)> endFunction
    );

    // Functions to start and stop timing on the commands when they're inserted/removed from the queue.
    static void startTiming(unique_ptr<BedrockCommand>& command);
    static void stopTiming(unique_ptr<BedrockCommand>& command);

    // Returns a list of all the method lines for all the requests currently queued. This function exists for state
    // reporting, and is called by BedrockServer when we receive a `Status` command.
    list<string> getRequestMethodLines();

    // Discards all commands scheduled more than msInFuture milliseconds after right now.
    void abandonFutureCommands(int msInFuture);

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    void push(unique_ptr<BedrockCommand>&& command);

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated. Allows for custom time scheduling.
    void push(unique_ptr<BedrockCommand>&& command, Scheduled time);
};
