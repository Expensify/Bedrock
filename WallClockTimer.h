#pragma once

// A WallClockTimer is a class that can be used by multiple threads to count wall clock time that some task is
// occurring. This won't double-count the same wall clock time for two different threads that are working in parallel.
class WallClockTimer {
  public:
    // Create a new WallClockTimer.
    WallClockTimer();

    // Start counting time. Has no effect if another thread is already counting time.
    void start();

    // Stop counting time. Has no effect if another thread is still counting time.
    void stop();

    // Return to time periods, the first being total wall clock time elapsed, and the second being the time that the
    // timer was actually running. Resets both these values upon call.
    // That is to say, if you call 'getStatsAndReset' every 10s, you should see pairs where the first value is always
    // approximately 10s, and the second value is always in the range 0-10s.
    pair<chrono::milliseconds, chrono::milliseconds> getStatsAndReset();

  private:
    mutex _m;
    uint64_t _count;
    chrono::steady_clock::time_point _currentStart;
    chrono::steady_clock::time_point _absoluteStart;
    chrono::milliseconds _elapsedRecorded;
};

class AutoScopedWallClockTimer {
  public:
    AutoScopedWallClockTimer(WallClockTimer& timer) :
        _timer(timer)
    {
        _timer.start();
    }

    ~AutoScopedWallClockTimer() {
        _timer.stop();
    }

  private:
    WallClockTimer& _timer;
};
