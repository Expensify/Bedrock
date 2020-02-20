#pragma once
#include <libstuff/libstuff.h>
#include <libstuff/SScheduledPriorityQueue.h>
#include "BedrockCommand.h"

class BedrockCommandQueue : public SScheduledPriorityQueue<BedrockCommandPtr> {
  public:
    BedrockCommandQueue();

    // Functions to start and stop timing on the commands when they're inserted/removed from the queue.
    static void startTiming(BedrockCommandPtr& command);
    static void stopTiming(BedrockCommandPtr& command);
    
    // Returns a list of all the method lines for all the requests currently queued. This function exists for state
    // reporting, and is called by BedrockServer when we receive a `Status` command.
    list<string> getRequestMethodLines();

    // Discards all commands scheduled more than msInFuture milliseconds after right now.
    void abandonFutureCommands(int msInFuture);

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    void push(BedrockCommandPtr&& command);
};
