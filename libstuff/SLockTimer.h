#pragma once

// A class for monitoring the amount of time spent in a given lock.
// To work properly, it requires that the lock is always accessed via this wrapper.
template<typename LOCKTYPE>
class SLockTimer : public SPerformanceTimer {
  public:
    SLockTimer(string description, LOCKTYPE& lock, uint64_t logIntervalSeconds = 60);
    ~SLockTimer();

    // Wrappers around calls to the equivalent functions for the underlying lock, but with timing info added.
    void lock();
    void unlock();

  private:
    int _lockCount;
    LOCKTYPE& _lock;
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
    if (!_lockCount) {
        start();
    }
    ++_lockCount;
}

template<typename LOCKTYPE>
void SLockTimer<LOCKTYPE>::unlock()
{
    --_lockCount;
    if (!_lockCount) {
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
