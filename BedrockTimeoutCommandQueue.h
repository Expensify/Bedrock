#include <libstuff/libstuff.h>
#include <BedrockCommand.h>

class BedrockTimeoutCommandQueue : public SSynchronizedQueue<BedrockCommand> {
  public:
    // Override the base class to account for timeouts.
    const BedrockCommand& front() const;
    void push(BedrockCommand&& rhs);
    BedrockCommand pop();

  private:
    // Map of timeouts to commands in the queue. Because the queue is a std::list, we can store iterators into it and
    // they stay valid as we manipulate the list, avoiding walking the list to re-locate them.
    multimap<uint64_t, list<BedrockCommand>::iterator> _timeoutMap;
};
