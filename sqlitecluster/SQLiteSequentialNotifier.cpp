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
        if (_globalResult != RESULT::UNKNOWN) {
            return _globalResult;
        } else if (state->result != RESULT::UNKNOWN) {
            return state->result;
        }
        state->waitingThreadConditionVariable.wait(lock);
    }
}

void SQLiteSequentialNotifier::notifyThrough(uint64_t value) {
    lock_guard<mutex> lock(_internalStateMutex);
    if (value > _value) {
        _value = value;
    }
    auto lastToDelete = _valueToPendingThreadMap.end();
    for (auto it = _valueToPendingThreadMap.begin(); it != _valueToPendingThreadMap.end(); it++) {
        if (it->first > value)  {
            // If we've passed our value, there's nothing else to erase, so we can stop.
            break;
        }

        // Note that we'll delete this item from the map.
        lastToDelete = it;

        // Make the changes to the state object - mark it complete and notify anyone waiting.
        lock_guard<mutex> lock(it->second->waitingThreadMutex);
        it->second->result = RESULT::COMPLETED;
        it->second->waitingThreadConditionVariable.notify_all();
    }

    // Now we've finished with all of our updates and notifications and can remove everything from our map.
    if (lastToDelete != _valueToPendingThreadMap.end()) {
        _valueToPendingThreadMap.erase(_valueToPendingThreadMap.begin(), lastToDelete);
    }
}

void SQLiteSequentialNotifier::cancel() {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::CANCELED;
    for (auto& p : _valueToPendingThreadMap) {
        lock_guard<mutex> lock(p.second->waitingThreadMutex);
        p.second->waitingThreadConditionVariable.notify_all();
    }
    _valueToPendingThreadMap.clear();
    _value = 0;
}

void SQLiteSequentialNotifier::checkpointRequired(SQLite& db) {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::CHECKPOINT_REQUIRED;
    for (auto& p : _valueToPendingThreadMap) {
        lock_guard<mutex> lock(p.second->waitingThreadMutex);
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
}
