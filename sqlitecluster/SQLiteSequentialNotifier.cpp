#include "SQLiteSequentialNotifier.h"

bool SQLiteSequentialNotifier::waitFor(uint64_t value) {
    shared_ptr<WaitState> state(nullptr);
    {
        lock_guard<mutex> lock(_m);
        if (value <= _value) {
            return true;
        }
        auto entry = _pending.find(value);
        if (entry == _pending.end()) {
            entry = _pending.emplace(value, make_shared<WaitState>()).first;
        }
        state = entry->second;
    }
    while (true) {
        unique_lock<mutex> lock(state->m);
        if (_canceled) {
            return false;
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
    while (!_pending.empty() && _pending.begin()->first <= value) {
        lock_guard<mutex> lock(_pending.begin()->second->m);
        _pending.begin()->second->completed = true;
        _pending.begin()->second->cv.notify_all();
        _pending.erase(_pending.begin());
    }
}

void SQLiteSequentialNotifier::cancel() {
    lock_guard<mutex> lock(_m);
    _canceled = true;
    for (auto& p : _pending) {
        lock_guard<mutex> lock(p.second->m);
        p.second->cv.notify_all();
    }
    _pending.clear();
    _value = 0;
}

void SQLiteSequentialNotifier::reset() {
    lock_guard<mutex> lock(_m);
    _canceled = false;
    _value = 0;
}
