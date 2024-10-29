#include <libstuff/libstuff.h>
#include "SQLiteSequentialNotifier.h"

SQLiteSequentialNotifier::RESULT SQLiteSequentialNotifier::waitFor(uint64_t value,  bool insideTransaction) {
    shared_ptr<WaitState> state(nullptr);
    {
        lock_guard<mutex> lock(_internalStateMutex);
        if (value <= _value) {
            return RESULT::COMPLETED;
        }

        // Create a new WaitState object and save a shared_ptr to it in `state`.
        state = make_shared<WaitState>();
        if (insideTransaction) {
            _valueToPendingThreadMap.emplace(value, state);
        } else {
            _valueToPendingThreadMapNoCurrentTransaction.emplace(value, state);
        }
    }

    while (true) {
        unique_lock<mutex> lock(state->waitingThreadMutex);
        if (_globalResult == RESULT::CANCELED) {
            if (_cancelAfter != 0 && value <= _cancelAfter) {
                // If cancelAfter is set, but higher than what we're waiting for, we ignore the CANCELED and wait for
                // this WaitState to have a result anyway.
                if (state->result != RESULT::UNKNOWN) {
                    return state->result;
                }
                // If there's no result yet, log that we're waiting for it.
                SINFO("Canceled after " << _cancelAfter << ", but waiting for " << value << " so not returning yet.");
            } else {
                // Canceled and we're not before the cancellation cutoff.
                return RESULT::CANCELED;
            }
        } else if (_globalResult != RESULT::UNKNOWN) {
            return _globalResult;
        } else if (state->result != RESULT::UNKNOWN) {
            return state->result;
        }
        SINFO("Waiting on " << value);
        cv_status result = state->waitingThreadConditionVariable.wait_for(lock, 1s);
        SINFO("Done waiting on " << value);
        if (result == cv_status::timeout) {
            // We shouldn't need this 1s timeout at all, and should be able to wait indefinitely until this thread is woken up, because that should always happen eventually. But it seems
            // like there might be a bug *somewhere* that causes us to either miss a notification that we've canceled some outstanding transactions, or that we are failing to notify them
            // that they're canceled. To handle that case, we wake up every second and will re-check, but warn if such a thing has happened.
            //
            // Note that this can also happen in the case of successful notifications - it seems like we can get a timeout even in the case that the notification was sent, if these two
            // things happen more-or-less simultaneously. The condition_variable documentation does not make it clear if this should be the case or not, but in every examined case, the
            // waited-for commit and the timeout happened more or less simultaneously (not with up to a 1s gap between them, which would indicate a missed notification and eventual timeout)
            // so we are ignoring that case which seems to work OK.
            //
            // We should investigate any instances of thew below logline to see if they're same as for the success cases mentioned above (i.e., the timeout happens simultaneously as the
            // cancellation) or if the log line is delayed by up to a second (indicating a problem).
            if (_globalResult == RESULT::CANCELED || state->result == RESULT::CANCELED) {
                // It's possible that we hit the timeout here after `cancel()` has set the global value, but before we received the notification.
                // This isn't a problem, and we can jump back to the top of the loop and check again. If there's some problem, we'll see it there.
                SINFO("Hit 1s timeout while global cancel " << (_globalResult == RESULT::CANCELED) << " or " << " specific cancel " << (state->result == RESULT::CANCELED));
                continue;
            }
        }
    }
}

uint64_t SQLiteSequentialNotifier::getValue() {
    lock_guard<mutex> lock(_internalStateMutex);
    return _value;
}

void SQLiteSequentialNotifier::notifyThrough(uint64_t value) {
    lock_guard<mutex> lock(_internalStateMutex);
    SINFO("notify through called for value " << value);
    if (value > _value) {
        _value = value;
    }
    
    // This thread is waiting on commit 414 before it can start. It will commit transaction 425.
    // 2024-10-29T23:27:51.752802+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteNode.cpp:211) _replicate [replicate786] [info] {cluster_node_2/FOLLOWING} [performance] BEGIN_TRANSACTION replicate thread for commit 425 waiting on DB count 414 (ASYNC)
    // This thread is waiting on commit 421 before it can start. It will commit transaction 427.
    // 2024-10-29T23:27:51.752842+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteNode.cpp:211) _replicate [replicate787] [info] {cluster_node_2/FOLLOWING} [performance] BEGIN_TRANSACTION replicate thread for commit 427 waiting on DB count 421 (ASYNC)
    // The first thread is now waiting for thread 424 to complete.
    // 2024-10-29T23:27:51.954400+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:41) waitFor [replicate786] [info] Waiting on 424
    // 
    // 414 is the last notification we get, so seeing what happened with the thread handling 415 would be interesting.
    // replicate836, replicate838 and replicate845 are waiting on 415.
    // 
    // There is no line like:
    // BEGIN_TRANSACTION replicate thread for commit 425 waiting on DB count 415
    // Either the message is dropped, or the thread never started. We should log the commit we would be starting a therad for in the parent thread.
    // That could narrow down one of those conditions.

    for (auto valueThreadMapPtr : {&_valueToPendingThreadMap, &_valueToPendingThreadMapNoCurrentTransaction}) {
        auto& valueThreadMap = *valueThreadMapPtr;
        auto lastToDelete = valueThreadMap.begin();
        for (auto it = valueThreadMap.begin(); it != valueThreadMap.end(); it++) {
            if (it->first > value)  {
                // If we've passed our value, there's nothing else to erase, so we can stop.
                SINFO("[performance] Breaking out of thread notifications because " <<  it->first << " > " << value);
                break;
            }

            // Note that we'll delete this item from the map.
            lastToDelete++;

            // Make the changes to the state object - mark it complete and notify anyone waiting.
            lock_guard<mutex> lock(it->second->waitingThreadMutex);
            it->second->result = RESULT::COMPLETED;
            SINFO("Notifying all threads waiting of value " << it->first);
            it->second->waitingThreadConditionVariable.notify_all();
        }

        // Now we've finished with all of our updates and notifications and can remove everything from our map.
        // Note that erasing an empty range (i.e., from() begin to begin()) is tested to be a no-op. The documentation I've
        // found for multimap is unclear on this, though the documentation for `std::list` specifies:
        // "The iterator first does not need to be dereferenceable if first==last: erasing an empty range is a no-op."
        //
        // I think it's reasonable to assume this is the intention for multimap as well, and in my testing, that was the
        // case.
        valueThreadMap.erase(valueThreadMap.begin(), lastToDelete);
    }
}

void SQLiteSequentialNotifier::cancel(uint64_t cancelAfter) {
    SINFO("Canceling all pending transactions after " << cancelAfter);
    
    // OK, we cancel everything after 1027.
    // This is because:
    // 2024-10-29T23:27:51.987857+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:73) notifyThrough [replicate1322] [info] notify through called for value 1027
    // 2024-10-29T23:27:54.869544+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteNode.cpp:1867) _changeState [sync] [info] {cluster_node_2/FOLLOWING} Replication threads should exit, canceling commits after current leader commit 1027
    // 
    // Everyone was notified about 1027, why do we have anyone waiting from before that? They could have started waiting after the
    // notification happened, but those should get handled.
    // 
    // Starts just before the notification:
    // 2024-10-29T23:27:51.867174+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:41) waitFor [replicate1036] [info] Waiting on 676
    // 2024-10-29T23:27:52.867740+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:43) waitFor [replicate1036] [info] Done waiting on 676
    // It's already done by the cancel.
    // 
    // Then it gets into a loop doing this:
    // 2024-10-29T23:27:55.868816+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:43) waitFor [replicate1036] [info] Done waiting on 676
    // 2024-10-29T23:27:55.868824+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:59) waitFor [replicate1036] [info] Hit 1s timeout while global cancel 1 or  specific cancel 0
    // 2024-10-29T23:27:55.868831+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:31) waitFor [replicate1036] [info] Canceled after 1027, but waiting for 676 so not returning yet.
    // 2024-10-29T23:27:55.868838+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:41) waitFor [replicate1036] [info] Waiting on 676
    // 
    // We don't get very many notifyThrough's.
    // 2024-10-29T23:27:55.010547+00:00 expensidev2004 bedrock10007: xxxxxx (SQLiteSequentialNotifier.cpp:73) notifyThrough [replicate1324] [info] notify through called for value 730
    // We de get the above, though, why doesn't that trigger this to stop waiting?
    // 
    // Also, why are we not getting tons of those?
    // Perhaps all the threads are just on previous ones so can't finish.


    lock_guard<mutex> lock(_internalStateMutex);

    // It's important that _cancelAfter is set before _globalResult. This avoids a race condition where we check
    // _globalResult in waitFor but then find _cancelAfter unset.
    _cancelAfter = cancelAfter;
    _globalResult = RESULT::CANCELED;

    for (auto valueThreadMapPtr : {&_valueToPendingThreadMap, &_valueToPendingThreadMapNoCurrentTransaction}) {
        auto& valueThreadMap = *valueThreadMapPtr;
        // If cancelAfter is specified, start from that value. Otherwise, we start from the beginning.
        auto start = _cancelAfter ? valueThreadMap.upper_bound(_cancelAfter) : valueThreadMap.begin();
        SINFO("[performance] Next value to cancel after " << cancelAfter << " is " << start->first);
        if (start == valueThreadMap.end()) {
            // There's nothing to remove.
            return;
        }

        // Now iterate across whatever's remaining and mark it canceled.
        auto current = start;
        while(current != valueThreadMap.end()) {
            SINFO("[performance] Setting canceled for thread waiting on " << current->first);
            lock_guard<mutex> lock(current->second->waitingThreadMutex);
            current->second->result = RESULT::CANCELED;
            current->second->waitingThreadConditionVariable.notify_all();
            current++;
            SINFO("[performance] Canceled for thread waiting on " << current->first);
        }

        // And remove these items entirely.
        valueThreadMap.erase(start, valueThreadMap.end());
    }
    SINFO("Canceled all pending transactions after " << cancelAfter);
}

void SQLiteSequentialNotifier::reset() {
    lock_guard<mutex> lock(_internalStateMutex);
    _globalResult = RESULT::UNKNOWN;
    _value = 0;
    _cancelAfter = 0;
}
