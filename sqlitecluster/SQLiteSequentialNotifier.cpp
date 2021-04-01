#include "SQLiteSequentialNotifier.h"

SQLiteSequentialNotifier::RESULT SQLiteSequentialNotifier::waitFor(uint64_t value,  bool insideTransaction) {
    shared_ptr<WaitState> state(nullptr);
    {
        lock_guard<mutex> lock(_internalStateMutex);
        if (value <= _value) {
            return RESULT::COMPLETED;
        }

        // Create a new WaitState object and save a shared_ptr to it in `state`.
        state = make_shared<WaitState>();
        if (insideTransaction) {
            _valueToPendingThreadMap.emplace(value, state);
        } else {
            _valueToPendingThreadMapNoCurrentTransaction.emplace(value, state);
        }
    }
    while (true) {
        unique_lock<mutex> lock(state->waitingThreadMutex);
        if (_globalResult == RESULT::CANCELED) {
            if (_cancelAfter != 0 && value <= _cancelAfter) {
                // If cancelAfter is set, but higher than what we're waiting for, we ignore the CANCELED and wait for
                // this WaitState to have a result anyway.
                if (state->result != RESULT::UNKNOWN) {
                    return state->result;
                }
                // If there's no result yet, log that we're waiting for it.
                SINFO("Canceled after " << _cancelAfter << ", but waiting for " << value << " so not returning yet.");
            } else {
                // Canceled and we're not before the cancellation cutoff.
                return RESULT::CANCELED;
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
    for (auto valueThreadMapPtr : {&_valueToPendingThreadMap, &_valueToPendingThreadMapNoCurrentTransaction}) {
        auto& valueThreadMap = *valueThreadMapPtr;
        auto lastToDelete = valueThreadMap.begin();
        for (auto it = valueThreadMap.begin(); it != valueThreadMap.end(); it++) {
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
        // found for multimap is unclear on this, though the documentation for `std::list` specifies:
        // "The iterator first does not need to be dereferenceable if first==last: erasing an empty range is a no-op."
        //
        // I think it's reasonable to assume this is the intention for multimap as well, and in my testing, that was the
        // case.
        valueThreadMap.erase(valueThreadMap.begin(), lastToDelete);
    }
}

void SQLiteSequentialNotifier::cancel(uint64_t cancelAfter) {
    lock_guard<mutex> lock(_internalStateMutex);

    // It's important that _cancelAfter is set before _globalResult. This avoids a race condition where we check
    // _globalResult in waitFor but then find _cancelAfter unset.
    _cancelAfter = cancelAfter;
    _globalResult = RESULT::CANCELED;

    for (auto valueThreadMapPtr : {&_valueToPendingThreadMap, &_valueToPendingThreadMapNoCurrentTransaction}) {
        auto& valueThreadMap = *valueThreadMapPtr;
        // If cancelAfter is specified, start from that value. Otherwise, we start from the beginning.
        auto start = _cancelAfter ? valueThreadMap.upper_bound(_cancelAfter) : valueThreadMap.begin();
        if (start == valueThreadMap.end()) {
            // There's nothing to remove.
            return;
        }

        // Now iterate across whatever's remaining and mark it canceled.
        auto current = start;
        while(current != valueThreadMap.end()) {
            lock_guard<mutex> lock(current->second->waitingThreadMutex);
            current->second->result = RESULT::CANCELED;
            current->second->waitingThreadConditionVariable.notify_all();
            current++;
        }

        // And remove these items entirely.
        valueThreadMap.erase(start, valueThreadMap.end());
    }
}

void SQLiteSequentialNotifier::checkpointRequired() {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::CHECKPOINT_REQUIRED;
    for (auto& p : _valueToPendingThreadMap) {
        lock_guard<mutex> lock(p.second->waitingThreadMutex);
        p.second->result = RESULT::CHECKPOINT_REQUIRED;
        auto start = STimeNow();
        p.second->waitingThreadConditionVariable.notify_all();
        SINFO("[checkpoint] Notified all threads waiting on a checkpoint in " << ((STimeNow() - start) / 1000) << "ms");
    }
    _valueToPendingThreadMap.clear();
    if (_valueToPendingThreadMapNoCurrentTransaction.size()) {
        SINFO("[checkpoint] Not unblocking threads waiting with no transaction.");
    }
}

void SQLiteSequentialNotifier::checkpointComplete() {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::UNKNOWN;
}

void SQLiteSequentialNotifier::reset() {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::UNKNOWN;
    _value = 0;
    _cancelAfter = 0;
}
