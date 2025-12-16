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
    map <string, chrono::steady_clock::duration> _defaults;
    map <string, chrono::steady_clock::duration> _totals;
};
