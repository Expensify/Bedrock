#pragma once
#include <list>
#include <map>
#include <memory>

#include <BedrockCommand.h>
#include <libstuff/SSynchronizedQueue.h>

class BedrockTimeoutCommandQueue : public SSynchronizedQueue<unique_ptr<BedrockCommand>> {
public:
    // Override the base class to account for timeouts.
    const unique_ptr<BedrockCommand>& front() const;
    void push(unique_ptr<BedrockCommand>&& rhs);
    unique_ptr<BedrockCommand> pop();

private:
    // Map of timeouts to commands in the queue. Because the queue is a list, we can store iterators into it and
    // they stay valid as we manipulate the list, avoiding walking the list to re-locate them.
    multimap<uint64_t, list<unique_ptr<BedrockCommand>>::iterator> _timeoutMap;
};
