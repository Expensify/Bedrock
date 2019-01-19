#include <libstuff/libstuff.h>
#include "SPerformanceTimer.h"

SPerformanceTimer::SPerformanceTimer(string description, bool reverse, uint64_t logIntervalSeconds)
  : _reverse(reverse), _logPeriod(logIntervalSeconds * STIME_US_PER_S), _lastStart(0), _lastStop(0),
    _lastLogStart(0), _timeLogged(0), _timeNotLogged(0), _description(description)
{}

void SPerformanceTimer::start() {
    uint64_t timestamp = STimeNow();

    // We're about to enter poll(), so if we've exited poll() before, then increment the time spent not polling. This
    // should always be true except the first time this is called.
    if (_lastStop) {
        _timeNotLogged += timestamp - _lastStop;
    }

    // Record the last time startPoll was called (i.e., right now).
    _lastStart = timestamp;

    // This records the time that we first start running this timer, if it's never been set before. From here forward,
    // we'll record a log line every "_logPeriod" microseconds.
    if (!_lastLogStart) {
        _lastLogStart = timestamp;
    }
}

void SPerformanceTimer::stop() {
    uint64_t timestamp = STimeNow();

    // We just exited poll(), so if we've recorded the time when we entered poll(), we'll increment the time spent
    // polling. Note that if `_lastStart` isn't set at this point, this class is being used incorrectly (i.e, you
    // called stopPoll() without calling startPoll().
    if (_lastStart) {
        _timeLogged += timestamp - _lastStart;
    }

    // Record the last time stopPoll was called (i.e., right now).
    _lastStop = timestamp;

    // If it's been longer than our log period, log our current statistics and start over on the next iteration.
    if (_lastLogStart + _logPeriod < timestamp) {
        log();
        _lastLogStart = timestamp;
        _timeLogged = 0;
        _timeNotLogged = 0;
    }
}

void SPerformanceTimer::log() {
    // Don't log if we didn't record anything.
    if (_timeLogged + _timeNotLogged == 0) {
        return;
    }

    // Compute the percentage of time we've been busy since the last log period started, as a friendly floating point
    // number with two decimal places.
    string adj;
    double percentage;
    if (_reverse) {
        percentage = 100.0 * ((double)_timeNotLogged / (double)(_timeLogged + _timeNotLogged));
        adj = "active";
    } else {
        percentage = 100.0 * ((double)_timeLogged / (double)(_timeLogged + _timeNotLogged));
        adj = "other";
    }
    char buffer[7];
    snprintf(buffer, 7, "%.2f", percentage);

    // Log both raw numbers and our friendly percentage.
    SINFO("[performance] " << (_timeLogged + _timeNotLogged)/1000 << "ms elapsed, " << _timeLogged/1000 << "ms in "
          << _description << ", " << _timeNotLogged/1000 << "ms " << adj << ". " << buffer << "% " << "usage.");
}
