#include "SQLiteCore.h"
    
SQLiteCore::SQLiteCore(SQLite& db) : _db(db)
{ }

bool SQLiteCore::commitCommand(SQLiteCommand& command)
{
    // Grab the global SQLite lock.
    SQLITE_COMMIT_AUTOLOCK;

    // This should always succeed.
    SASSERT(_db.prepare());

    // If there's nothing to commit, we won't bother, but warn, as we should have noticed this after calling `process`.
    if (_db.getUncommittedHash().empty()) {
        SWARN("Commit called with nothing to commit.");
        return true;
    }

    int errorCode = _db.commit();
    if (errorCode == SQLITE_BUSY_SNAPSHOT) {
        SINFO("Commit conflict, rolling back.");
        _db.rollback();
        return false;
    }
    // Success! Let the node know something's been committed, and return.
    SQLiteNode::unsentTransactions.store(true);
    return true;
}
