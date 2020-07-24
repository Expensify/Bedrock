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
    while (!_valueToPendingThreadMap.empty() && _valueToPendingThreadMap.begin()->first <= value) {
        lock_guard<mutex> lock(_valueToPendingThreadMap.begin()->second->waitingThreadMutex);
        _valueToPendingThreadMap.begin()->second->result = RESULT::COMPLETED;
        _valueToPendingThreadMap.begin()->second->waitingThreadConditionVariable.notify_all();
        _valueToPendingThreadMap.erase(_valueToPendingThreadMap.begin());
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
