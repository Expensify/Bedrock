#pragma once

// A class for monitoring the amount of time spent in a given lock.
// To work properly, it requires that the lock is always accessed via this wrapper.
template<typename LOCKTYPE>
class SLockTimer : public SPerformanceTimer {
  public:
    SLockTimer(string description, LOCKTYPE& lock, uint64_t logIntervalSeconds = 10);
    ~SLockTimer();

    // Wrappers around calls to the equivalent functions for the underlying lock, but with timing info added.
    void lock();
    void unlock();

  private:
    atomic<int> _lockCount;
    LOCKTYPE& _lock;
    recursive_mutex _syncMutex;
};

template<typename LOCKTYPE>
SLockTimer<LOCKTYPE>::SLockTimer(string description, LOCKTYPE& lock, uint64_t logIntervalSeconds)
  : SPerformanceTimer(description, false, logIntervalSeconds), _lockCount(0), _lock(lock)
{ }

template<typename LOCKTYPE>
SLockTimer<LOCKTYPE>::~SLockTimer() {
}

template<typename LOCKTYPE>
void SLockTimer<LOCKTYPE>::lock()
{
    _lock.lock();

    // We atomically increment the counter, and only start the timer if we were the first to do so, in the case
    // multiple threads are competing for this.
    SAUTOLOCK(_syncMutex);
    int count = _lockCount.fetch_add(1);
    if (!count) {
        start();
    }
}

template<typename LOCKTYPE>
void SLockTimer<LOCKTYPE>::unlock()
{
    SAUTOLOCK(_syncMutex);
    int count = _lockCount.fetch_sub(1);

    // Count contains the value just before our decrement. If it was 1, that means we're now at a lock count of 0, and
    // can stop the timer.
    if (count == 1) {
        stop();
    }
    _lock.unlock();
}

template<typename TIMERTYPE> 
class SLockTimerGuard {
  public:
    SLockTimerGuard(TIMERTYPE& lockTimer) : _lockTimer(lockTimer) {
        _lockTimer.lock();
    };
    ~SLockTimerGuard() {
        _lockTimer.unlock();
    }

  private:
    TIMERTYPE& _lockTimer;
};
