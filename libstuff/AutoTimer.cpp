/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    AutoTimer.cpp
 * Path:    libstuff/AutoTimer.cpp
 * Pair:    AutoTimer.h
 *
 * INTENT
 *   Implements AutoTimer/AutoTimerTime; see the header for the public
 *   contract.
 *
 * OBJECTS
 *   AutoTimer::AutoTimer      - seeds the interval-start clock.
 *   AutoTimer::start          - marks the instance's start time.
 *   AutoTimer::stop           - adds the instance's elapsed time to
 *                               `_countedTime`; every 10s past
 *                               `_intervalStart`, logs the counted/elapsed
 *                               ratio via SINFO and resets the interval.
 *   AutoTimerTime::AutoTimerTime / ~AutoTimerTime - call start()/stop() on
 *                               the referenced AutoTimer for RAII scoping.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Fits repo convention. Redefining SLOGPREFIX locally is a slightly
 *   unusual pattern for a one-file logging tweak but is harmless and
 *   self-contained.
 * ─────────────────────────────────────────────────────────────────────*/
#include "AutoTimer.h"
#include <libstuff/libstuff.h>

#undef SLOGPREFIX
#define SLOGPREFIX "{} "

AutoTimer::AutoTimer(const string& name) : _name(name), _intervalStart(chrono::steady_clock::now()), _countedTime(0)
{
}

void AutoTimer::start()
{
    _instanceStart = chrono::steady_clock::now();
}

void AutoTimer::stop()
{
    auto stopped = chrono::steady_clock::now();
    _countedTime += stopped - _instanceStart;
    if (stopped > (_intervalStart + 10s)) {
        auto counted = chrono::duration_cast<chrono::milliseconds>(_countedTime).count();
        auto elapsed = chrono::duration_cast<chrono::milliseconds>(stopped - _intervalStart).count();
        static char percent[10] = {0};
        snprintf(percent, 10, "%.2f", static_cast<double>(counted) / static_cast<double>(elapsed) * 100.0);
        SINFO("[performance] AutoTimer (" << _name << "): " << counted << "/" << elapsed << " ms timed, " << percent << "%");
        _intervalStart = stopped;
        _countedTime = chrono::microseconds::zero();
    }
};

AutoTimerTime::AutoTimerTime(AutoTimer& t) : _t(t)
{
    _t.start();
}

AutoTimerTime::~AutoTimerTime()
{
    _t.stop();
}
