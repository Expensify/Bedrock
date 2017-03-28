#pragma once

// A class for monitoring the amount of time spent in a given lock.
// To work properly, it requires that the lock is always accessed via this wrapper.
template<typename LOCKTYPE>
class SLockTimer : public SPerformanceTimer {
  public:
    SLockTimer(string description, LOCKTYPE& lock, uint64_t logIntervalSeconds = 60);
    ~SLockTimer();

    void lock();
    void unlock();

    // For testing.
    void stop();

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
void SLockTimer<LOCKTYPE>::stop()
{
    if (_lastStart) {
        uint64_t current = STimeNow() - _lastStart;
        if (current > 10 * 1000 * 1000) {
            SWARN("[concurrent] Over 10S spent in Commit Lock: " << current << "us.");
            SLogStackTrace();
        }
    }
    SPerformanceTimer::stop();
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
