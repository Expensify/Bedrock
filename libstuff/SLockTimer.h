#pragma once

// A class for monitoring the amount of time spent in a given lock.
// To work properly, it requires that the lock is always accessed via this wrapper.
template<typename LOCKTYPE>
class SLockTimer : public SPerformanceTimer {
  public:
    static atomic<bool> enableExtraLogging;
    SLockTimer(string description, LOCKTYPE& lock, uint64_t logIntervalSeconds = 10);
    ~SLockTimer();

    // Wrappers around calls to the equivalent functions for the underlying lock, but with timing info added.
    void lock();
    void unlock();

    // We override the base class log function.
    virtual void log();

  private:
    atomic<int> _lockCount;
    LOCKTYPE& _lock;

    // Each thread keeps it's own counter of wait and lock time.
    map<string, pair<int,int>> _perThreadTiming;
};

template<typename LOCKTYPE>
atomic<bool> SLockTimer<LOCKTYPE>::enableExtraLogging(false);

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
    uint64_t waitStart = STimeNow();
    _lock.lock();
    uint64_t waitEnd = STimeNow();

    // We atomically increment the counter, and only start the timer if we were the first to do so, in the case
    // we're calling this recursively.
    int count = _lockCount.fetch_add(1);
    if (!count) {
        uint64_t waitElapsed = waitEnd - waitStart;

        // We're locking, go ahead and update the per-thread map. This is already synchronized behind `_lock`, so no
        // need to grab a second mutex.
        auto it = _perThreadTiming.find(SThreadLogName);
        if (it == _perThreadTiming.end()) {
            // We didn't find an entry for this thread name, so we'll insert one with the calculated wait time, and a
            // lock time of 0.
            _perThreadTiming.emplace(make_pair(SThreadLogName, make_pair((waitElapsed), 0)));
        } else {
            // We already have an entry, add to the wait time.
            it->second.first += (waitElapsed);
        }
        if (enableExtraLogging.load() && waitElapsed > 1000000) {
            SWARN("[performance] Over 1s spent waiting for lock " << _description << ": " << waitElapsed << "us.");
            SLogStackTrace();
        }
        start();
    }
}

template<typename LOCKTYPE>
void SLockTimer<LOCKTYPE>::unlock()
{
    int count = _lockCount.fetch_sub(1);

    // Count contains the value just before our decrement. If it was 1, that means we're now at a lock count of 0, and
    // can stop the timer.
    if (count == 1) {
        stop();
        uint64_t lockElapsed = _lastStop - _lastStart;

        // We're still holding `_lock`, so no further synchronization is required for the per-thread map.
        auto it = _perThreadTiming.find(SThreadLogName);
        if (it != _perThreadTiming.end()) {
            // We already have an entry, add to the lock time.
            it->second.second += lockElapsed;
        } else {
            SWARN("Unlocking without ever locking.");
        }
        if (enableExtraLogging.load() && lockElapsed > 1000000) {
            SWARN("[performance] Over 1s spent waiting in lock " << _description << ": " << lockElapsed << "us.");
        }
    }
    _lock.unlock();
}

template<typename LOCKTYPE>
void SLockTimer<LOCKTYPE>::log() {

    // When called inside `stop()`, as by the base class, this is thread safe as it's protected by `_lock`. When called
    // from anywhere else, it's up to the caller to protect this!

    for (auto& pair : _perThreadTiming) {
        uint64_t waitTime = pair.second.first;
        uint64_t lockTime = pair.second.second;
        uint64_t freeTime = _timeLogged + _timeNotLogged - waitTime - lockTime;

        // Catch overflow.
        if (_timeLogged + _timeNotLogged < waitTime + lockTime) {
            freeTime = 0;
        }

        string threadName = pair.first;

        // Compute the percentage of time we've been busy since the last log period started, as a friendly floating point
        // number with two decimal places.
        double lockPercent = 0;
        double waitPercent = 0;
        double freePercent = 0;
        if (_reverse) {
            // we don't handle `reverse` for SLockTimer.
            SWARN("`reverse` flag incorrectly specified for SLockTimer.");
        } else {
            lockPercent = 100.0 * ((double)lockTime / (_timeLogged + _timeNotLogged));
            waitPercent = 100.0 * ((double)waitTime / (_timeLogged + _timeNotLogged));
            freePercent = 100.0 * ((double)freeTime / (_timeLogged + _timeNotLogged));
        }
        char lockBuffer[7] = {0};
        snprintf(lockBuffer, 7, "%.2f", lockPercent);
        char waitBuffer[7] = {0};
        snprintf(waitBuffer, 7, "%.2f", waitPercent);
        char freeBuffer[7] = {0};
        snprintf(freeBuffer, 7, "%.2f", freePercent);

        // Log both raw numbers and our friendly lockPercent.
        SINFO("[performance] " << _description << ", thread: " << threadName << ". Wait/Lock/Free " << waitTime << "/"
              << lockTime << "/" << freeTime << "us, " << waitBuffer << "/" << lockBuffer << "/" << freeBuffer
              << "%.");

        // Reset the counters.
        pair.second.first = 0;
        pair.second.second = 0;
    }

    // Call the base class log function as well.
    SPerformanceTimer::log();
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
