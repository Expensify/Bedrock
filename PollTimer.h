#pragma once
class PollTimer {
  public:
    PollTimer(uint64_t logIntervalSeconds = 60);
    void startPoll();
    void stopPoll();
    void log();

  private:
    uint64_t _logPeriod;
    uint64_t _lastStart;
    uint64_t _lastStop;
    uint64_t _lastLogStart;
    uint64_t _timeInPoll;
    uint64_t _timeNotInPoll;
};
