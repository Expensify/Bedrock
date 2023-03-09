#pragma once
#include <libstuff/libstuff.h>
#include "SQLite.h"
#include <condition_variable>

class SQLiteSequentialNotifier {
  public:

    // Enumeration of all the possible states to result from waiting.
    enum class RESULT {
        UNKNOWN,
        COMPLETED,
        CANCELED,
    };

    // Constructor
    SQLiteSequentialNotifier() : _value(0), _globalResult(RESULT::UNKNOWN), _cancelAfter(0) {}

    // Blocks until `_value` meets or exceeds `value`, unless an exceptional case (CANCELED, CHEKPOINT_REQUIRED) is
    // hit, and returns the corresponding RESULT.
    SQLiteSequentialNotifier::RESULT waitFor(uint64_t value, bool insideTransaction);

    // Causes any threads waiting for a value up to and including `value` to return `true`.
    void notifyThrough(uint64_t value);

    // Causes any thread waiting for any value to return `false`. Also, any future calls to `waitFor` will return
    // `RESULT::CANCELED` until `reset` is called.
    // If `cancelAfter` is specified, then only threads waiting for a value *greater than* cancelAfter are interrupted,
    // and only calls to `waitFor` with values higher than the current _value return `RESULT::CANCELED`.
    void cancel(uint64_t cancelAfter = 0);

    // Returns the current value of this notifier.
    uint64_t getValue();

    // After calling `reset`, all calls to `waitFor` return `false` until this is called, and then they will wait
    // again. This allows for a caller to call `cancel`, wait for the completion of their threads, and then call
    // `reset` to use the object again.
    void reset();

  private:
    // This encapsulates the set of values we need to have a thread wait. It's a mutex and condition_variable that the
    // thread can use to wait, and a result indicating if the required result has actually been reached (because
    // condition_variables can be spuriously interrupted and need a second `wait()` call).
    struct WaitState {
        WaitState() : result(RESULT::UNKNOWN) {}
        mutex waitingThreadMutex;
        condition_variable waitingThreadConditionVariable;
        RESULT result;
    };

    mutex _internalStateMutex;
    multimap<uint64_t, shared_ptr<WaitState>> _valueToPendingThreadMap;
    multimap<uint64_t, shared_ptr<WaitState>> _valueToPendingThreadMapNoCurrentTransaction;
    uint64_t _value;

    // If there is a global result for all pending operations (i.e., they've been canceled), that is stored here.
    atomic<RESULT> _globalResult;

    // For saving the value after which new or existing waiters will be returned a CANCELED result.
    atomic<uint64_t> _cancelAfter;
};
