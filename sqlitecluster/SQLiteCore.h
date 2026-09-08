/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SQLiteCore.h
 * Path:    sqlitecluster/SQLiteCore.h
 * Pair:    SQLiteCore.cpp
 *
 * INTENT
 *   Wraps commit/rollback of a SQLite database with the cluster-aware checks
 *   needed to do so safely: only commit while still the leader, and run an
 *   optional plugin-notification hook during prepare.
 *
 * OBJECTS
 *   SQLiteCore  - commits or rolls back a transaction on a wrapped SQLite&, refusing to commit if the node is no longer leading
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits: a small piece of cluster-aware commit logic, alongside SQLite and
 *   SQLiteNode in sqlitecluster.
 *
 * NAMING QUALITY
 *   The `notificationHandler` function-pointer parameter names its own
 *   SQLite argument `_db`, shadowing the name of this class's `_db` member
 *   in spirit even though it's a distinct callback parameter.
 * ─────────────────────────────────────────────────────────────────────*/
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

    // Commit the outstanding transaction on the DB.
    // Returns true on successful commit, false on conflict.
    bool commit(const SQLiteNode& node, uint64_t& commitID, string& transactionHash, const string& commandName, bool needsPluginNotifiation, void (*notificationHandler)(SQLite& _db, int64_t tableID) = nullptr, chrono::microseconds commitLockTimeout = chrono::hours(24), atomic<bool>* abortPtr = nullptr) noexcept;

    // Roll back a transaction if we've decided not to commit it.
    void rollback(const string& commandName = "NONE");

protected:
    SQLite& _db;
};
