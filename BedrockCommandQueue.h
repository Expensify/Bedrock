#pragma once
class BedrockCommand;

class BedrockCommandQueue {
  public:
    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    void push(BedrockCommand&& item);

    // Get an item from the queue. Optionally, a timeout can be specified.
    // If timeout is non-zero, and an exception will be thrown after timeoutUS microseconds, if no work was available.
    BedrockCommand get(uint64_t timeoutUS = 0);

    // Returns true if there are no queued commands.
    bool empty();

    // Looks for a command with the given ID and removes it.
    // This will inspect every command in the case the command does not exist.
    bool removeByID(const string& id);

    // Returns a list of all the method lines for all the requests currently queued. This function exists for state
    // reporting, and is called when we receive a `Status` command.
    list<string> getRequestMethodLines();

  private:
    // Removes and returns the first workable command in the queue. A command is workable if:
    // 1. It's executeTimestamp is not in the future.
    //
    // First means: Starting from the highest priority queue, the command with the oldest executeTimestamp.
    // all commands of priority N+1 are before commands of priority N, regardless of timestamp.
    //
    // This function throws if no workable commands are available.
    BedrockCommand _dequeue();

    // Synchronization primitives for managing access to the queue.
    mutex _queueMutex;
    condition_variable _queueCondition;

    // The priority queue in which we store commands.
    map<int, multimap<uint64_t, BedrockCommand>> _commandQueue;
};
