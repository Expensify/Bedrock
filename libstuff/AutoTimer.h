/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    AutoTimer.h
 * Path:    libstuff/AutoTimer.h
 * Pair:    AutoTimer.cpp
 *
 * INTENT
 *   Accumulates time spent inside repeated start/stop intervals and, every
 *   10 seconds, logs what percentage of wall-clock time was spent inside
 *   those intervals. AutoTimerTime is the RAII helper that calls
 *   start/stop for a scope.
 *
 * OBJECTS
 *   AutoTimer      - owns the running total and the 10s logging interval;
 *                     start()/stop() bracket a timed section.
 *   AutoTimerTime  - RAII guard: calls AutoTimer::start on construction and
 *                     AutoTimer::stop on destruction.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits, but see NAMING QUALITY below.
 *
 * NAMING QUALITY
 *   [CANDIDATE] The header's own comment flags that BedrockCore.h declares
 *   an unrelated, differently-scoped class also named `AutoTimer` (a
 *   per-command timing guard). Same name, same directory-adjacent codebase,
 *   different purpose — genuinely confusable and worth a rename on one
 *   side. Otherwise consistent with repo convention.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <chrono>
#include <string>
using namespace std;

// There is a *different* AutoTimer in BedrockCore, which is annoying.
class AutoTimer {
public:
    AutoTimer(const string& name);
    void start();
    void stop();

private:
    string _name;
    chrono::steady_clock::time_point _intervalStart;
    chrono::steady_clock::time_point _instanceStart;
    chrono::steady_clock::duration _countedTime;
};

class AutoTimerTime {
public:
    AutoTimerTime(AutoTimer& t);
    ~AutoTimerTime();

private:
    AutoTimer& _t;
};
