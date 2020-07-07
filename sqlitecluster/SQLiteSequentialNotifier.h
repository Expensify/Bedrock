#pragma once
#include <libstuff/libstuff.h>

class SQLiteSequentialNotifier {
  public:
    SQLiteSequentialNotifier() : _value(0) {}
    bool waitFor(uint64_t value);
    void notifyThrough(uint64_t value);
    void cancel();

  private:
    struct WaitState {
        WaitState() : canceled(false), completed(false) {}
        mutex m;
        condition_variable cv;
        bool canceled;
        bool completed;
    };

    mutex _m;
    map<uint64_t, shared_ptr<WaitState>> _pending;
    uint64_t _value;
};
