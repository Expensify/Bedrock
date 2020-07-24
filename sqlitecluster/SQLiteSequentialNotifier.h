#pragma once
#include <libstuff/libstuff.h>
#include "SQLite.h"

// This implements the CheckpointRequiredListener interface because we potentially need to interrupt transactions in
// the following situation:
//
// 1. Transaction B begins (for commit N).
// 2. A restart checkpoint begins, blocking new trasnactions.
// 3. Transaction A (for commit N - 1) attempts to begin (but blocks on the checkpoint).
//
// Transaction B can't finish until transaction A does, but transaction A can't start until the checkpoint completes.
// However, the checkpoint can't run until all pending transactions (B) complete, thus creating a deadlock.
//
// So SQLiteSequentialNotifier implements `CheckpointRequiredListener` so that when a checkpoint is required, calls to
// `waitFor` in transaction B will be interrupted and throw checkpoint_required_error, causing the transaction to be
// aborted and restarted, which unblocks the checkpoint. Then, the checkpoint will complete, transaction A can run, and
// checkpoint B can complete.
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
    map<uint64_t, shared_ptr<WaitState>> _valueToPendingThreadMap;
    uint64_t _value;
    bool _canceled;
    bool _checkpointRequired;
};
