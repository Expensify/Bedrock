#pragma once
#include <condition_variable>
#include <mutex>
#include <thread>

#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>

class BedrockServer;
class SQLite;

// Trims old rows out of the journal tables. One thread, so the deletes cannot conflict with each other, and a delete
// that does conflict with a commit costs nothing but a retry on a later pass.
class BedrockJournalDeleter {
public:
    BedrockJournalDeleter(BedrockServer& server);
    ~BedrockJournalDeleter();

    static inline atomic<bool> enableDeleterThread{true};
    static inline atomic<int64_t> deleterBatchSize{1000};

    // Runs on the committing thread: must not block or touch the database.
    void wake();

    // Does nothing unless the flag is on and the state is trimmable. Never stops the thread: this runs on the sync
    // thread, where a join could stall a failover, so the shutdown path is what actually stops it.
    void start(SQLiteNodeState state);

    void stop();

    void trimNextTable(SQLite& db);

private:
    static bool isTrimmableState(SQLiteNodeState state);

    BedrockServer& _server;
    size_t _nextTable = 0;

    // Serializes start against stop, so a restart can't clear _stop while stop() is still joining the old thread.
    mutex _lifecycleMutex;

    unique_ptr<thread> _thread;

    // Guards the two fields below between wake() and the thread's wait predicate.
    mutex _wakeMutex;
    condition_variable _cv;

    // A flag, not a count: a burst of commits collapses into one trim, and the next commit takes what is left.
    bool _wakeRequested = false;
    bool _stop = false;
};
