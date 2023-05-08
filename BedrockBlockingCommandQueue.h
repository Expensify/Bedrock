#pragma once
#include <libstuff/libstuff.h>
#include "BedrockCommandQueue.h"
#include "BedrockCommand.h"

class BedrockBlockingCommandQueue : public BedrockCommandQueue {
  public:
    BedrockBlockingCommandQueue();

    // Functions to start and stop timing on the commands when they're inserted/removed from the queue.
    static void startTiming(unique_ptr<BedrockCommand>& command);
    static void stopTiming(unique_ptr<BedrockCommand>& command);
};
