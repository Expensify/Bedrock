#pragma once
#include <libstuff/libstuff.h>
#include "SQLite.h"

class SQLiteSequentialNotifier : public SQLite::CheckpointRequiredListener {
  public:
    SQLiteSequentialNotifier() : _value(0), _canceled(false), _checkpointRequired(false) {}

    // Returns `true` when the counter matches or exceeds `value`, or `false` when someone calls `cancel`. Otherwise,
    // it will wait forever.
    bool waitFor(uint64_t value);

    // Causes any threads waiting for a value up to and including `value` to return `true`.
    void notifyThrough(uint64_t value);

    // Causes any thread waiting for any value to return `false`. Also, any future calls to `waitFor` will return
    // `false` until `reset` is called.
    void cancel();

    // Implement the base class to notify for checkpoints
    void checkpointRequired(SQLite& db) override;
    void checkpointComplete(SQLite& db) override;

    // After calling `reset`, all calls to `waitFor` return `false` until this is called, and then they will wait
    // again. This allows for a caller to call `cancel`, wait for the completion of their threads, and then call
    // `reset` to use the object again.
    void reset();

  private:
    struct WaitState {
        WaitState() : completed(false) {}
        mutex m;
        condition_variable cv;
        bool completed;
    };

    mutex _m;
    map<uint64_t, shared_ptr<WaitState>> _pending;
    uint64_t _value;
    bool _canceled;
    bool _checkpointRequired;
};
