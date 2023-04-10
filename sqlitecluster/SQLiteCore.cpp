#include <libstuff/libstuff.h>
#include "SQLiteCore.h"
#include "SQLite.h"
#include "SQLiteNode.h"
    
SQLiteCore::SQLiteCore(SQLite& db) : _db(db)
{ }

bool SQLiteCore::commit(const string& description, uint64_t& commitID, string& transactionHash) {
    // This should always succeed.
    SASSERT(_db.prepare(&commitID, &transactionHash));

    // If there's nothing to commit, we won't bother, but warn, as we should have noticed this already.
    if (_db.getUncommittedHash().empty()) {
        SWARN("Commit called with nothing to commit.");
        return true;
    }

    // Perform the actual commit, rollback if it fails.
    int errorCode = _db.commit(description);
    if (errorCode == SQLITE_BUSY_SNAPSHOT) {
        SINFO("Commit conflict, rolling back.");
        _db.rollback();
        return false;
    } else if (errorCode == SQLite::COMMIT_DISABLED) {
        SINFO("Commits currently disabled, rolling back.");
        _db.rollback();
        return false;
    }

    return true;
}

void SQLiteCore::rollback() {
    _db.rollback();
}
