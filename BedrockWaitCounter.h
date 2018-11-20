#include <libstuff/libstuff.h>

// TODO: This is the wrong place for these lock classes to live, but it was convenient for testing.
template <typename T>
class lock_guard_if {
  public:
    lock_guard_if(T& m, bool lock) : _mutex(m), _lock(lock) {
        if (_lock) {
            _mutex.lock();
        }
    }

    ~lock_guard_if() {
        if (_lock) {
            _mutex.unlock();
        }
    }

  private:
    T& _mutex;
    bool _lock;
};

template <typename T>
class shared_lock_if {
  public:
    shared_lock_if(T& m, bool lock) : _mutex(m), _lock(lock) {
        if (_lock) {
            _mutex.lock_shared();
        }
    }

    ~shared_lock_if() {
        if (_lock) {
            _mutex.unlock_shared();
        }
    }

  private:
    T& _mutex;
    bool _lock;
};

// A counter on which you can wait for the count to be below a certain value.
class BedrockWaitCounter {
  public:
    BedrockWaitCounter(int64_t startValue = 0);
    
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
