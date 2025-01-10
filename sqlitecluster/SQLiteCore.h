#pragma once
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
    bool commit(const SQLiteNode& node, uint64_t& commitID, string& transactionHash, bool needsPluginNotifiation, void (*notificationHandler)(SQLite& _db, int64_t tableID) = nullptr) noexcept;

    // Roll back a transaction if we've decided not to commit it.
    void rollback();

  protected:
    SQLite& _db;
};
