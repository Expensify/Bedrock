#include "BedrockWaitCounter.h"

BedrockWaitCounter::BedrockWaitCounter(int64_t startValue) : _value(startValue) {
}

int64_t BedrockWaitCounter::waitUntilLessThan(int64_t value) {
    while (true) {
        unique_lock<mutex> lock(_m);
        if (_value < value) {
            return _value;
        }
        _cv.wait(lock);
        if (_value < value) {
            return _value;
        }
    }
};

int64_t BedrockWaitCounter::waitUntilLessThanOrEqual(int64_t value) {
    while (true) {
        unique_lock<mutex> lock(_m);
        if (_value <= value) {
            return _value;
        }
        _cv.wait(lock);
        if (_value <= value) {
            return _value;
        }
    }
};

int64_t BedrockWaitCounter::waitUntilGreaterThan(int64_t value) {
    while (true) {
        unique_lock<mutex> lock(_m);
        if (_value > value) {
            return _value;
        }
        _cv.wait(lock);
        if (_value > value) {
            return _value;
        }
    }
};

int64_t BedrockWaitCounter::waitUntilGreaterThanOrEqual(int64_t value) {
    while (true) {
        unique_lock<mutex> lock(_m);
        if (_value >= value) {
            return _value;
        }
        _cv.wait(lock);
        if (_value >= value) {
            return _value;
        }
    }
};

// Pre-increment
int64_t BedrockWaitCounter::operator++() {
    int64_t retVal;
    {
        lock_guard<mutex> lock(_m);
        ++_value;
        retVal = _value;
    }
    _cv.notify_all();
    return retVal;
}

// Pre-decrement
int64_t BedrockWaitCounter::operator--() {
    int64_t retVal;
    {
        lock_guard<mutex> lock(_m);
        --_value;
        retVal = _value;
    }
    _cv.notify_all();
    return retVal;
}

// Post-increment
int64_t BedrockWaitCounter::operator++(int ignore) {
    int64_t retVal;
    {
        lock_guard<mutex> lock(_m);
        retVal = _value;
        _value++;
    }
    _cv.notify_all();
    return retVal;
}

// Post-decrement
int64_t BedrockWaitCounter::operator--(int ignore) {
    int64_t retVal;
    {
        lock_guard<mutex> lock(_m);
        retVal = _value;
        _value--;
    }
    _cv.notify_all();
    return retVal;
}

int64_t BedrockWaitCounter::value() {
    lock_guard<mutex> lock(_m);
    return _value;
}

