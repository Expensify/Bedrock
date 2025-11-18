#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <condition_variable>

class SQLitePool {
public:
    // Create a pool of DB handles.
    SQLitePool(size_t maxDBs, const string& filename, int cacheSize, int maxJournalSize, int minJournalTables,
               int64_t mmapSizeGB = 0, bool hctree = false, const string& checkpointMode = "PASSIVE");
    ~SQLitePool();

    // Get the base object (the first one created, which uses the `journal` table). Note that if called by multiple
    // threads, both threads may hold the same DB handle.
    SQLite& getBase();

    // Gets an index into the internal data structure for a handle that is marked as "inUse". If there are too many
    // "inUse" handles (maxDBs), this will wait until one is available.
    // If `createHandle` is true, and all existent handles are in use, but there is space for more handles, this will
    // create a new one and return it's index.
    // However, if `creteHandle` is false, this will *not* create the handle, but just reserve the index, and allow the
    // handle to be created later with `initializeIndex` on this slot.
    size_t getIndex(bool createHandle = true);

    // Takes an allocated index and creates the appropriate DB handle if required.
    SQLite& initializeIndex(size_t index);

    // Return an object to the pool.
    void returnToPool(size_t index);

private:
    // Synchronization variables.
    mutex _sync;
    condition_variable _wait;

    // Internal limit on the number of handles we'll allow. This exists to make sure we don't go over any
    // system-imposed limits on FDs.
    size_t _maxDBs;

    // Our base object that all others are based upon.
    SQLite _baseDB;

    // These are indexes into `_objects`.
    set<size_t> _availableHandles;
    set<size_t> _inUseHandles;

    // This is a vector of pointers to all possibly allocated objects.
    vector<SQLite*> _objects;
};

class SQLiteScopedHandle {
public:
    SQLiteScopedHandle(SQLitePool& pool, size_t index);
    ~SQLiteScopedHandle();
    SQLite& db();
    void release();

private:
    SQLitePool& _pool;
    size_t _index;
    bool _released;
};
