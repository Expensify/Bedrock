#pragma once
#include <libstuff/libstuff.h>
#include <libstuff/SScheduledPriorityQueue.h>
#include "BedrockCommand.h"

class BedrockCommandQueue : public SScheduledPriorityQueue<BedrockCommand> {
  public:
    BedrockCommandQueue();

    static void startTiming(BedrockCommand& item);
    static void stopTiming(BedrockCommand& item);
    
    // Returns a list of all the method lines for all the requests currently queued. This function exists for state
    // reporting, and is called by BedrockServer when we receive a `Status` command.
    list<string> getRequestMethodLines();

    // Discards all commands scheduled more than msInFuture milliseconds after right now.
    void abandonFutureCommands(int msInFuture);

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    void push(BedrockCommand&& item);
};
