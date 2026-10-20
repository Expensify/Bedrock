#pragma once
#include <atomic>
#include <chrono>
class SQLite;
class SQLiteNode;

#include <cstdint>
#include <string>

using namespace std;

class SQLiteCore {
public:
    // Constructor that stores the database object we'll be working on.
    SQLiteCore(SQLite& db);

    enum class COMMIT_RESULT
    {
        // The transaction committed.
        OK,

        // The transaction was rolled back because of a conflict, an abort, a lost leadership, or disabled commits.
        // The caller may retry it.
        CONFLICT,

        // The transaction was rolled back because the commit lock wasn't acquired before `commitLockTimeout`.
        TIMEOUT
    };

    // Commit the outstanding transaction on the DB, or roll it back if it can't commit.
    // On success, commitID and transactionHash identify the journal entry, or are zero/empty if none was recorded.
    COMMIT_RESULT commit(const SQLiteNode& node, uint64_t& commitID, string& transactionHash, const string& commandName, bool needsPluginNotifiation, void (*notificationHandler)(SQLite& _db, int64_t tableID) = nullptr, chrono::microseconds commitLockTimeout = chrono::hours(24), atomic<bool>* abortPtr = nullptr) noexcept;

    // Roll back a transaction if we've decided not to commit it.
    void rollback(const string& commandName = "NONE");

protected:
    SQLite& _db;
};
