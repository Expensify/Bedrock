#include "PollTimer.h"

PollTimer::PollTimer(uint64_t logIntervalSeconds)
  : _logPeriod(logIntervalSeconds * STIME_US_PER_S),
    _lastStart(0),
    _lastStop(0),
    _lastLogStart(0),
    _timeInPoll(0),
    _timeNotInPoll(0) { }

void PollTimer::startPoll() {
    uint64_t timestamp = STimeNow();

    if (_lastStop) {
        _timeNotInPoll += timestamp - _lastStop;
    }

    _lastStart = timestamp;
    if (!_lastLogStart) {
        _lastLogStart = timestamp;
    }
}

void PollTimer::stopPoll(bool force) {
    uint64_t timestamp = STimeNow();

    if (_lastStart) {
        _timeInPoll += timestamp - _lastStart;
    }

    _lastStop = timestamp;

    if (force || _lastLogStart + _logPeriod < timestamp) {
        double percentage = 100.0 * ((double)_timeNotInPoll / (_timeInPoll + _timeNotInPoll));
        char buffer[7];
        snprintf(buffer, 7, "%.2f", percentage);
        SINFO("[performance] " << (_timeInPoll + _timeNotInPoll) << "us elapsed, " << _timeInPoll
              << "us in poll(), " << _timeNotInPoll << "us busy. " << buffer << "% busy."
              << (force ? " (forced)" : ""));

        // Reset this to our period after our previous start time, not after the current time, to prevent slow skew as
        // poll doesn't return precisely on these boundaries.
        _lastLogStart += _logPeriod;
        _timeInPoll = 0;
        _timeNotInPoll = 0;
    }
}
