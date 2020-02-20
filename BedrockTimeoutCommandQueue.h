#include <libstuff/libstuff.h>
#include <BedrockCommand.h>

class BedrockTimeoutCommandQueue : public SSynchronizedQueue<BedrockCommandPtr> {
  public:
    // Override the base class to account for timeouts.
    const BedrockCommandPtr& front() const;
    void push(BedrockCommandPtr&& rhs);
    BedrockCommandPtr pop();

  private:
    // Map of timeouts to commands in the queue. Because the queue is a std::list, we can store iterators into it and
    // they stay valid as we manipulate the list, avoiding walking the list to re-locate them.
    multimap<uint64_t, list<BedrockCommandPtr>::iterator> _timeoutMap;
};
