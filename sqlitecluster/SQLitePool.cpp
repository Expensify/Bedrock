#include <libstuff/libstuff.h>
#include "SQLite.h"
#include "SQLitePool.h"

SQLitePool::SQLitePool(size_t maxDBs,
                       const string& filename,
                       int cacheSize,
                       int maxJournalSize,
                       int minJournalTables,
                       const string& synchronous,
                       int64_t mmapSizeGB,
                       bool enableChkptExclMode)
: _maxDBs(max(maxDBs, 1ul)),
  _baseDB(filename, cacheSize, maxJournalSize, minJournalTables, synchronous, mmapSizeGB, enableChkptExclMode),
  _objects(_maxDBs, nullptr)
{
}

SQLitePool::~SQLitePool() {
    lock_guard<mutex> lock(_sync);
    if (_inUseHandles.size()) {
        SWARN("Destroying SQLitePool with DBs in use.");
    }
    for (auto dbHandle : _availableHandles) {
        delete _objects[dbHandle];
        _objects[dbHandle] = nullptr;
    }
    for (auto dbHandle : _inUseHandles) {
        delete _objects[dbHandle];
        _objects[dbHandle] = nullptr;
    }
}

SQLite& SQLitePool::getBase() {
    return _baseDB;
}

size_t SQLitePool::getIndex(bool createHandle) {
    while (true) {
        unique_lock<mutex> lock(_sync);
        if (_availableHandles.size()) {
            // Return an existing handle.
            auto frontIt = _availableHandles.begin();
            size_t index = *frontIt;
            _inUseHandles.insert(index);
            _availableHandles.erase(frontIt);
            SDEBUG("Returning existing DB handle");
            return index;
        } else if (_availableHandles.size() + _inUseHandles.size() < (_maxDBs - 1)) {
            size_t index = _availableHandles.size() + _inUseHandles.size();
            _inUseHandles.insert(index);

            // Create a new handle unless we're not supposed to. We unlock here as we're no longer in a position to
            // change which indices are in use.
            lock.unlock();
            if (createHandle) {
                initializeIndex(index);
            }
            SINFO("Returning new DB handle: " << index);
            return index;
        } else {
            // Wait for a handle.
            SINFO("Waiting for DB handle");
            _wait.wait(lock);
        }
    }
}

SQLite& SQLitePool::initializeIndex(size_t index) {
    // We don't lock here on purpose. Because these indexes are handed out individually, no two threads should have the
    // same one, and thus they should independently be able to update the addresses in this vector, as neither will
    // change the allocation of the vector itself.
    // It's an error to run `initializeIndex` in two threads on the same index at the same time.
    if (_objects[index] == nullptr) {
        _objects[index] = new SQLite(_baseDB);
    }
    return *_objects[index];
}

void SQLitePool::returnToPool(size_t index) {
    {
        lock_guard<mutex> lock(_sync);
        _availableHandles.insert(index);
        _inUseHandles.erase(index);
        SDEBUG("DB handle returned to pool.");
    }
    _wait.notify_one();
}

SQLiteScopedHandle::SQLiteScopedHandle(SQLitePool& pool, size_t index) : _pool(pool), _index(index)
{} 

SQLiteScopedHandle::~SQLiteScopedHandle() {
    _pool.returnToPool(_index);
}

SQLite& SQLiteScopedHandle::db() {
    return _pool.initializeIndex(_index);
}
