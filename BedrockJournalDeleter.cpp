#include "BedrockJournalDeleter.h"

#include <BedrockServer.h>
#include <sqlitecluster/SQLitePool.h>

BedrockJournalDeleter::BedrockJournalDeleter(BedrockServer& server) : _server(server)
{
}

BedrockJournalDeleter::~BedrockJournalDeleter()
{
    stop();
}

void BedrockJournalDeleter::wake()
{
    {
        lock_guard<decltype(_wakeMutex)> lock(_wakeMutex);
        _wakeRequested = true;
    }
    _cv.notify_one();
}

bool BedrockJournalDeleter::isTrimmableState(SQLiteNodeState state)
{
    // We also commit while SYNCHRONIZING, but a node catching up drains what it accumulated once it is FOLLOWING.
    return state == SQLiteNodeState::LEADING || state == SQLiteNodeState::FOLLOWING || state == SQLiteNodeState::STANDINGDOWN;
}

void BedrockJournalDeleter::start(SQLiteNodeState state)
{
    if (!enableDeleterThread || !isTrimmableState(state)) {
        return;
    }

    lock_guard<decltype(_lifecycleMutex)> lifecycleLock(_lifecycleMutex);
    if (_thread) {
        return;
    }

    // Set before the thread exists, and any previous one was joined under _lifecycleMutex, so nothing reads this
    // while we write it. A wake left over from while we were stopped just costs one trim.
    _stop = false;
    _thread = make_unique<thread>([this]() {
        SInitialize("journalDeleter");
        SThreadLogCommand = "JournalDeleter";
        while (true) {
            {
                unique_lock<decltype(_wakeMutex)> lock(_wakeMutex);

                // No timeout on purpose: a cluster that isn't committing has nothing to trim.
                _cv.wait(lock, [this]() {
                    return _stop || _wakeRequested;
                });
                if (_stop) {
                    return;
                }
                _wakeRequested = false;
            }

            if (!isTrimmableState(_server.getState())) {
                continue;
            }

            // A failed trim throws, and this is a thread entry point, so letting it bubble up would terminate Bedrock.
            try {
                shared_ptr<SQLitePool> dbPool = _server.getDBPool();
                if (!dbPool) {
                    continue;
                }
                SQLiteScopedHandle dbScope(*dbPool, dbPool->getIndex());
                trimNextTable(dbScope.db());
            } catch (const exception& e) {
                SWARN("Journal trim failed, will retry on a later wake.", {{"what", e.what()}});
            } catch (...) {
                SWARN("Journal trim failed with an unknown exception, will retry on a later wake.");
            }
        }
    });
}

void BedrockJournalDeleter::stop()
{
    lock_guard<decltype(_lifecycleMutex)> lifecycleLock(_lifecycleMutex);

    unique_ptr<thread> localThread;
    {
        lock_guard<decltype(_wakeMutex)> lock(_wakeMutex);
        if (!_thread) {
            return;
        }
        _stop = true;
        localThread = move(_thread);
    }
    _cv.notify_one();

    // Joined outside the lock so the thread can take it on its way out.
    localThread->join();
}

void BedrockJournalDeleter::trimNextTable(SQLite& db)
{
    const size_t tableIndex = _nextTable++ % db.getJournalTableCount();
    const uint64_t start = STimeNow();
    if (!db.trimJournalTable(tableIndex, deleterBatchSize)) {
        // A commit landing underneath us surfaces as a failed COMMIT, which is expected under load.
        SINFO("Journal trim did not commit, will retry on a later pass.", {{"journalTableIndex", to_string(tableIndex)}});
        return;
    }

    const uint64_t timeSpent = STimeNow() - start;

    // This runs on every commit, so only log the slow ones.
    if (timeSpent > 100'000) {
        SINFO("Journal trim timing info", {
            {"totalTransactionElapsed", to_string(timeSpent / 1000)},
            {"journalTableIndex", to_string(tableIndex)},
            {"deleted", to_string(db.getLastWriteChangeCount())},
        });
    }
}
