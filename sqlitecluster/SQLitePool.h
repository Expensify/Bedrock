#pragma once
#include <libstuff/libstuff.h>
class SQLite;

class SQLitePool {
  public:
    // Create a pool of DB handles.
    SQLitePool(size_t maxDBs, const string& filename, int cacheSize, bool enableFullCheckpoints, int maxJournalSize,
               int createJournalTables, const string& synchronous = "", int64_t mmapSizeGB = 0, bool pageLoggingEnabled = false);
    ~SQLitePool();

    // Get the base object (the first one created, which uses the `journal` table). Note that if called by multiple
    // threads, both threads may hold the same DB handle.
    SQLite& getBase();

    // Get any object except the base. Will wait for an available handle if there are already maxDBs.
    SQLite& get();

    // Return an object to the pool.
    void returnToPool(SQLite& object);

  private:
    // Synchronization variables.
    mutex _sync;
    condition_variable _wait;

    // Internal limit on the number of handles we'll allow. This exists to make sure we don't go over any
    // system-imposed limits on FDs.
    size_t _maxDBs;

    // Pointers to all of our objects.
    SQLite* _baseDB;
    set<SQLite*> _availableHandles;
    set<SQLite*> _inUseHandles;
};

class SQLiteScopedHandle {
  public:
    SQLiteScopedHandle(SQLitePool& pool, SQLite& db);
    ~SQLiteScopedHandle();
    SQLite& db();

  private:
    SQLitePool& _pool;
    SQLite& _db;
};
