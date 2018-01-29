#pragma once
class BedrockCommand;

class BedrockCommandQueue {
  public:
    class timeout_error : exception {
      public:
        const char* what() const noexcept {
            return "timeout";
        }
    };

    // Remove all items from the queue.
    void clear();

    // Returns true if there are no queued commands.
    bool empty();

    // Returns the size of the queue.
    size_t size();

    // Get an item from the queue. Optionally, a timeout can be specified.
    // If timeout is non-zero, an exception will be thrown after timeoutUS microseconds, if no work was available.
    BedrockCommand get(uint64_t timeoutUS = 0);

    // Returns a list of all the method lines for all the requests currently queued. This function exists for state
    // reporting, and is called by BedrockServer when we receive a `Status` command.
    list<string> getRequestMethodLines();

    // Add an item to the queue. The queue takes ownership of the item and the caller's copy is invalidated.
    void push(BedrockCommand&& item);

    // Looks for a command with the given ID and removes it.
    // This will inspect every command in the case the command does not exist.
    bool removeByID(const string& id);

  private:
    // Removes and returns the first workable command in the queue. A command is workable if it's executeTimestamp is
    // not in the future.
    //
    // "First" means: Of all workable commands, the one in the highest priority queue, with the lowest timestamp of any
    //                command *in that priority queue* - i.e., priority trumps timestamp.
    //
    // This function throws an exception if no workable commands are available.
    BedrockCommand _dequeue();

    // Synchronization primitives for managing access to the queue.
    mutex _queueMutex;
    condition_variable _queueCondition;

    // The priority queue in which we store commands. This is a map of integer priorities to their respective maps.
    // Each of those maps maps timestamps to commands.
    map<int, multimap<uint64_t, BedrockCommand>> _commandQueue;

    // Returns the size of the queue, only counting items that aren't scheduled in the future, optionally counting all
    // the items in the queue. This function does *not* lock _queueMutex, as it's only intended to be called
    // internally.
    size_t _runnableSize(size_t* totalSize);
};
