#pragma once

class SPerformanceTimer {
  public:
    SPerformanceTimer(string description, bool reverse = false, uint64_t logIntervalSeconds = 60);
    void start();
    void stop();
    void log();

  protected:
    bool _reverse;
    uint64_t _logPeriod;
    uint64_t _lastStart;
    uint64_t _lastStop;
    uint64_t _lastLogStart;
    uint64_t _timeLogged;
    uint64_t _timeNotLogged;
    string _description;
};
