#include "SQLiteSequentialNotifier.h"

SQLiteSequentialNotifier::RESULT SQLiteSequentialNotifier::waitFor(uint64_t value) {
    shared_ptr<WaitState> state(nullptr);
    {
        lock_guard<mutex> lock(_internalStateMutex);
        if (value <= _value) {
            return RESULT::COMPLETED;
        }

        // Create a new WaitState object and save a shared_ptr to it in `state`.
        state = make_shared<WaitState>();
        _valueToPendingThreadMap.emplace(value, state);
    }
    while (true) {
        unique_lock<mutex> lock(state->waitingThreadMutex);
        if (_globalResult == RESULT::CANCELED) {
            if (_cancelAfter != 0 && value <= _cancelAfter) {
                // We can just keep going here, it's canceled after what we're waiting for.
                SINFO("Canceled after " << _cancelAfter << " but we're only waiting for " << value << " so continuing.");
            } else {
                // Canceled and we're not before the cancellation cutoff.
                return _globalResult;
            }
        } else if (_globalResult != RESULT::UNKNOWN) {
            return _globalResult;
        } else if (state->result != RESULT::UNKNOWN) {
            return state->result;
        }
        state->waitingThreadConditionVariable.wait(lock);
    }
}

uint64_t SQLiteSequentialNotifier::getValue() {
    lock_guard<mutex> lock(_internalStateMutex);
    return _value;
}

void SQLiteSequentialNotifier::notifyThrough(uint64_t value) {
    lock_guard<mutex> lock(_internalStateMutex);
    if (value > _value) {
        _value = value;
    }
    auto lastToDelete = _valueToPendingThreadMap.begin();
    for (auto it = _valueToPendingThreadMap.begin(); it != _valueToPendingThreadMap.end(); it++) {
        if (it->first > value)  {
            // If we've passed our value, there's nothing else to erase, so we can stop.
            break;
        }

        // Note that we'll delete this item from the map.
        lastToDelete++;

        // Make the changes to the state object - mark it complete and notify anyone waiting.
        lock_guard<mutex> lock(it->second->waitingThreadMutex);
        it->second->result = RESULT::COMPLETED;
        it->second->waitingThreadConditionVariable.notify_all();
    }

    // Now we've finished with all of our updates and notifications and can remove everything from our map.
    // Note that erasing an empty range (i.e., from() begin to begin()) is tested to be a no-op. The documentation I've
    // fond for multimap is unclear on this, though the documentation for `std::list` specifies:
    // "The iterator first does not need to be dereferenceable if first==last: erasing an empty range is a no-op."
    //
    // I think it's reasonable to assume this is the intention for multimap as well, and in my testing, that was the
    // case.
    _valueToPendingThreadMap.erase(_valueToPendingThreadMap.begin(), lastToDelete);
}

void SQLiteSequentialNotifier::cancel(uint64_t cancelAfter) {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::CANCELED;

    // If cancelAfter is specified, start from that value. Otherwise, we start from the beginning.
    auto start = cancelAfter ? _valueToPendingThreadMap.upper_bound(cancelAfter) : _valueToPendingThreadMap.begin();
    if (start == _valueToPendingThreadMap.end()) {
        // There's nothing to remove.
        return;
    }

    // Now iterate across whatever's remaining and mark it canceled.
    auto current = start;
    while(current != _valueToPendingThreadMap.end()) {
        lock_guard<mutex> lock(current->second->waitingThreadMutex);
        SINFO("Canceling: " << current->first);
        current->second->result = RESULT::CANCELED;
        current->second->waitingThreadConditionVariable.notify_all();
        current++;
    }

    // And remove these items entirely.
    _valueToPendingThreadMap.erase(start, _valueToPendingThreadMap.end());
}

void SQLiteSequentialNotifier::checkpointRequired(SQLite& db) {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::CHECKPOINT_REQUIRED;
    for (auto& p : _valueToPendingThreadMap) {
        lock_guard<mutex> lock(p.second->waitingThreadMutex);
        p.second->result = RESULT::CHECKPOINT_REQUIRED;
        p.second->waitingThreadConditionVariable.notify_all();
    }
    _valueToPendingThreadMap.clear();
}

void SQLiteSequentialNotifier::checkpointComplete(SQLite& db) {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::UNKNOWN;
}

void SQLiteSequentialNotifier::reset() {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::UNKNOWN;
    _value = 0;
    _cancelAfter = 0;
}
