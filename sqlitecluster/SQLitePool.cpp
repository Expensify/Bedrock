#include "SQLite.h"
#include "SQLitePool.h"

SQLitePool::SQLitePool(size_t maxDBs,
                       const string& filename,
                       int cacheSize,
                       bool enableFullCheckpoints,
                       int maxJournalSize,
                       int minJournalTables,
                       const string& synchronous,
                       int64_t mmapSizeGB,
                       bool pageLoggingEnabled)
: _maxDBs(max(maxDBs, 1ul)),
  _baseDB(filename, cacheSize, enableFullCheckpoints, maxJournalSize, minJournalTables, synchronous, mmapSizeGB, pageLoggingEnabled)
{ }

SQLitePool::~SQLitePool() {
    lock_guard<mutex> lock(_sync);
    if (_inUseHandles.size()) {
        SWARN("Destroying SQLitePool with DBs in use.");
    }
    for (auto dbHandle : _availableHandles) {
        delete dbHandle;
    }
    for (auto dbHandle : _inUseHandles) {
        delete dbHandle;
    }
}

SQLite& SQLitePool::getBase() {
    return _baseDB;
}

SQLite& SQLitePool::get() {
    while (true) {
        unique_lock<mutex> lock(_sync);
        if (_availableHandles.size()) {
            // Return an existing handle.
            auto frontIt = _availableHandles.begin();
            SQLite* db = *frontIt;
            _inUseHandles.insert(db);
            _availableHandles.erase(frontIt);
            SINFO("Returning existing DB handle");
            return *db;
        } else if (_availableHandles.size() + _inUseHandles.size() < (_maxDBs - 1)) {
            // Create a new handle.
            SQLite* db = new SQLite(_baseDB);
            _inUseHandles.insert(db);
            SINFO("Returning new DB handle: " << (_availableHandles.size() + _inUseHandles.size()));
            return *db;
        } else {
            // Wait for a handle.
            SINFO("Waiting for DB handle");
            _wait.wait(lock);
        }
    }
}

void SQLitePool::returnToPool(SQLite& object) {
    {
        lock_guard<mutex> lock(_sync);
        _availableHandles.insert(&object);
        _inUseHandles.erase(&object);
        SINFO("DB handle returned to pool.");
    }
    _wait.notify_one();
}

SQLiteScopedHandle::SQLiteScopedHandle(SQLitePool& pool, SQLite& db) : _pool(pool), _db(db)
{} 

SQLiteScopedHandle::~SQLiteScopedHandle() {
    _pool.returnToPool(_db);
}

SQLite& SQLiteScopedHandle::db() {
    return _db;
}
