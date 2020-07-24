#include "SQLiteSequentialNotifier.h"

bool SQLiteSequentialNotifier::waitFor(uint64_t value) {
    shared_ptr<WaitState> state(nullptr);
    {
        lock_guard<mutex> lock(_m);
        if (value <= _value) {
            return true;
        }
        auto entry = _valueToPendingThreadMap.find(value);
        if (entry == _valueToPendingThreadMap.end()) {
            entry = _valueToPendingThreadMap.emplace(value, make_shared<WaitState>()).first;
        }
        state = entry->second;
    }
    while (true) {
        unique_lock<mutex> lock(state->m);
        if (_canceled) {
            return false;
        } else if (_checkpointRequired) {
            throw SQLite::checkpoint_required_error();
        } else if (state->completed) {
            return true;
        }
        state->cv.wait(lock);
    }
}

void SQLiteSequentialNotifier::notifyThrough(uint64_t value) {
    lock_guard<mutex> lock(_m);
    if (value > _value) {
        _value = value;
    }
    while (!_valueToPendingThreadMap.empty() && _valueToPendingThreadMap.begin()->first <= value) {
        lock_guard<mutex> lock(_valueToPendingThreadMap.begin()->second->m);
        _valueToPendingThreadMap.begin()->second->completed = true;
        _valueToPendingThreadMap.begin()->second->cv.notify_all();
        _valueToPendingThreadMap.erase(_valueToPendingThreadMap.begin());
    }
}

void SQLiteSequentialNotifier::cancel() {
    lock_guard<mutex> lock(_m);
    _canceled = true;
    for (auto& p : _valueToPendingThreadMap) {
        lock_guard<mutex> lock(p.second->m);
        p.second->cv.notify_all();
    }
    _valueToPendingThreadMap.clear();
    _value = 0;
}

void SQLiteSequentialNotifier::checkpointRequired(SQLite& db) {
    lock_guard<mutex> lock(_m);
    _checkpointRequired = true;
    for (auto& p : _valueToPendingThreadMap) {
        lock_guard<mutex> lock(p.second->m);
        p.second->cv.notify_all();
    }
    _valueToPendingThreadMap.clear();
}

void SQLiteSequentialNotifier::checkpointComplete(SQLite& db) {
    lock_guard<mutex> lock(_m);
    _canceled = false;
    _checkpointRequired = false;
}

void SQLiteSequentialNotifier::reset() {
    lock_guard<mutex> lock(_m);
    _canceled = false;
    _checkpointRequired = false;
    _value = 0;
}
