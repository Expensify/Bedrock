/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SPerformanceTimer.h
 * Path:    libstuff/SPerformanceTimer.h
 * Pair:    SPerformanceTimer.cpp
 *
 * INTENT
 *   Accumulates wall-clock time spent in named sub-phases of some larger
 *   operation, then periodically logs a percentage breakdown so an engineer
 *   can see where time within a repeating task is actually going.
 *
 * OBJECTS
 *   SPerformanceTimer  - tracks start/stop intervals per named "type",
 *                         sums durations, and logs a breakdown every 10s.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits. A small measurement utility alongside libstuff's other generic
 *   helpers.
 *
 * NAMING QUALITY
 *   Consistent with repo convention (S-prefix, leading-underscore members).
 *   `log`'s parameter name `elapsed` shadows the concept already tracked
 *   internally as `_lastLogStart`-derived time, but the two uses are close
 *   enough in meaning that it reads fine.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <libstuff/libstuff.h>

class SPerformanceTimer {
public:
    SPerformanceTimer(const string& description, const map<string, chrono::steady_clock::duration>& defaults = {});
    void start(const string& type);
    uint64_t stop();
    void log(chrono::steady_clock::duration elapsed);

protected:
    string _description;
    chrono::steady_clock::time_point _lastStart;
    chrono::steady_clock::time_point _lastLogStart;
    string _lastType;
    map<string, chrono::steady_clock::duration> _defaults;
    map<string, chrono::steady_clock::duration> _totals;
};
