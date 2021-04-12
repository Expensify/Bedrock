#include <libstuff/libstuff.h>
#include "WallClockTimer.h"

WallClockTimer::WallClockTimer() :
  _count(0),
  _currentStart(),
  _absoluteStart(),
  _elapsedRecorded(chrono::milliseconds::zero())
{
}

void WallClockTimer::start() {
    lock_guard<mutex> lock(_m);

    // If the timer isn't running, start it.
    if (!_count) {
        _currentStart = chrono::steady_clock::now();
    }
    _count++;
    
    // If we've never started this timer before, mark this as its absolute beginning.
    if (_absoluteStart.time_since_epoch() == chrono::milliseconds::zero()) {
        _absoluteStart = _currentStart;
    }
}

void WallClockTimer::stop() {
    lock_guard<mutex> lock(_m);

    // If we're about to decrement to zero, record the time.
    if (_count == 1) {
        // No longer timing, record the length of time that we were.
        _elapsedRecorded += chrono::duration_cast<std::chrono::milliseconds>(chrono::steady_clock::now() - _currentStart);
    } else if (_count == 0) {
        SWARN("Stopped timer that wasn't running. Resetting.");
        _count = 0;
        _absoluteStart = chrono::steady_clock::time_point();
        _currentStart = chrono::steady_clock::time_point();
        _elapsedRecorded = chrono::milliseconds::zero();
    }
    _count--;
}

pair<chrono::milliseconds, chrono::milliseconds> WallClockTimer::getStatsAndReset() {
    lock_guard<mutex> lock(_m);

    // Figure out how much time it's been.
    auto now = chrono::steady_clock::now();
    auto startTime = _absoluteStart;
    auto recorded = _elapsedRecorded;
    if (_count) {
        recorded += chrono::duration_cast<std::chrono::milliseconds>(now - _currentStart);
    }

    // reset our counters.
    _currentStart = now;
    _absoluteStart = now;
    _elapsedRecorded = chrono::milliseconds::zero();

    // Return our result.
    return make_pair(chrono::duration_cast<std::chrono::milliseconds>(now - startTime), recorded);
}
