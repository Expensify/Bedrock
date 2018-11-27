#include <libstuff/libstuff.h>

// A counter on which you can wait for the count to be below a certain value.
class SWaitCounter {
  public:
    SWaitCounter(int64_t startValue = 0);
    
    // Return the new value of the counter.
    int64_t waitUntilLessThan(int64_t value);
    int64_t waitUntilLessThanOrEqual(int64_t value);
    int64_t waitUntilGreaterThan(int64_t value);
    int64_t waitUntilGreaterThanOrEqual(int64_t value);

    // Pre-increment
    int64_t operator++();

    // Pre-decrement
    int64_t operator--();

    // Post-increment
    int64_t operator++(int);

    // Post-decrement
    int64_t operator--(int);

    // Get the current value.
    int64_t value();

  private:
    int64_t _value;

    // Locking primitives.
    mutex _m;
    condition_variable _cv;
};

// Increments the counter either at construction, or if deferred, when `inc()` is called.
// Decrements the counter on destruction, if it was ever incremented.
class SWaitCounterScopedIncrement {
  public:
    SWaitCounterScopedIncrement(SWaitCounter& counter, bool defer = false) : _counter(counter), _incremented(!defer) {
        if (_incremented) {
            ++_counter;
        }
    }

    ~SWaitCounterScopedIncrement() {
        if (_incremented) {
            --_counter;
        }
    }

    // Increments the counter if it wasn't already incremented.
    int64_t inc() {
        if (!_incremented) {
            _incremented = true;
            return ++_counter;
        }
        return _counter.value();
    }

    // Increments the counter if it was already incremented.
    int64_t dec() {
        if (_incremented) {
            _incremented = false;
            return --_counter;
        }
        return _counter.value();
    }

  private:
    SWaitCounter& _counter;
    bool _incremented;
};
