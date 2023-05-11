#pragma once
#include "BedrockCommandQueue.h"

class BedrockCommand;

class BedrockBlockingCommandQueue : public BedrockCommandQueue {
  public:
    BedrockBlockingCommandQueue();

    // Functions to start and stop timing on the commands when they're inserted/removed from the queue.
    static void startTiming(unique_ptr<BedrockCommand>& command);
    static void stopTiming(unique_ptr<BedrockCommand>& command);
};
