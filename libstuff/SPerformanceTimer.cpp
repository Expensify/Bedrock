#include <libstuff/libstuff.h>
#include "SPerformanceTimer.h"

SPerformanceTimer::SPerformanceTimer(const string& description, const map<string, chrono::steady_clock::duration>& defaults)
    : _description(description),
    _lastLogStart(chrono::steady_clock::now()),
    _defaults(defaults),
    _totals(_defaults)
{
}

void SPerformanceTimer::start(const string& type)
{
    _lastType = type;
    _lastStart = chrono::steady_clock::now();
}

void SPerformanceTimer::stop()
{
    // Get the time, and the time since last start.
    auto now = chrono::steady_clock::now();
    auto duration = now - _lastStart;

    // Record this time.
    auto it = _totals.find(_lastType);
    if (it != _totals.end()) {
        it->second += now - _lastStart;
    } else {
        _totals.emplace(_lastType, duration);
    }

    // Now log, if required.
    if (now - _lastLogStart > 10s) {
        log(now - _lastLogStart);

        // Reset.
        _lastLogStart = now;
        _totals = _defaults;
    }
}

void SPerformanceTimer::log(chrono::steady_clock::duration elapsed)
{
    auto elapsedUS = chrono::duration_cast<chrono::microseconds>(elapsed).count();
    chrono::steady_clock::duration accounted(chrono::steady_clock::duration::zero());
    list<string> results;
    char buffer[100];
    for (auto& p : _totals) {
        double microsecs = chrono::duration_cast<chrono::microseconds>(p.second).count();
        double percentage = (microsecs / elapsedUS) * 100.0;
        snprintf(buffer, 100, "%s %.2fms (%.2f%%)", p.first.c_str(), (microsecs / 1000), percentage);
        results.emplace_back(buffer);
        accounted += p.second;
    }
    SINFO(_description << ": " << SComposeList(results) << " in "
          << chrono::duration_cast<chrono::milliseconds>(elapsed).count() << "ms total.");
}
