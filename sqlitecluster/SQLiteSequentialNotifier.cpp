#include <libstuff/libstuff.h>
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
        cv_status result = state->waitingThreadConditionVariable.wait_for(lock, 1s);
        if (result == cv_status::timeout) {
            // We shouldn't need this 1s timeout at all, and should be able to wait indefinitely until this thread is woken up, because that should always happen eventually. But it seems
            // like there might be a bug *somewhere* that causes us to either miss a notification that we've canceled some outstanding transactions, or that we are failing to notify them
            // that they're canceled. To handle that case, we wake up every second and will re-check, but warn if such a thing has happened.
            //
            // Note that this can also happen in the case of successful notifications - it seems like we can get a timeout even in the case that the notification was sent, if these two
            // things happen more-or-less simultaneously. The condition_variable documentation does not make it clear if this should be the case or not, but in every examined case, the
            // waited-for commit and the timeout happened more or less simultaneously (not with up to a 1s gap between them, which would indicate a missed notification and eventual timeout)
            // so we are ignoring that case which seems to work OK.
            //
            // We should investigate any instances of thew below logline to see if they're same as for the success cases mentioned above (i.e., the timeout happens simultaneously as the
            // cancellation) or if the log line is delayed by up to a second (indicating a problem).
            if (_globalResult == RESULT::CANCELED || state->result == RESULT::CANCELED) {
                SWARN("Got timeout in wait_for but state has changed! Was waiting for " << value);
                return RESULT::CANCELED;
            }
        }
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

void SQLiteSequentialNotifier::reset() {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::UNKNOWN;
    _value = 0;
    _cancelAfter = 0;
}
