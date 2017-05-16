#include "SQLiteCore.h"
#include "SQLite.h"
#include "SQLiteNode.h"
    
SQLiteCore::SQLiteCore(SQLite& db) : _db(db)
{ }

bool SQLiteCore::commit(bool extraLogging) {
    // Grab the global SQLite lock.
    SQLITE_COMMIT_AUTOLOCK;

    // This should always succeed.
    SASSERT(_db.prepare());

    // If there's nothing to commit, we won't bother, but warn, as we should have noticed this already.
    if (_db.getUncommittedHash().empty()) {
        SWARN("Commit called with nothing to commit.");
        return true;
    }

    // Perform the actual commit, rollback if it fails.
    int errorCode = _db.commit(extraLogging);
    if (errorCode == SQLITE_BUSY_SNAPSHOT) {
        SINFO("Commit conflict, rolling back.");
        _db.rollback();
        return false;
    }

    // Success! Let the node know something's been committed, and return.
    SQLiteNode::unsentTransactions.store(true);
    return true;
}

void SQLiteCore::rollback() {
    _db.rollback();
}
