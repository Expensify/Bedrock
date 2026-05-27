#include <libstuff/AutoScopeOnPrepare.h>
#include <libstuff/libstuff.h>
#include "SQLiteCore.h"
#include "SQLite.h"
#include "SQLiteNode.h"

SQLiteCore::SQLiteCore(SQLite& db) : _db(db)
{
}

bool SQLiteCore::commit(const SQLiteNode& node, uint64_t& commitID, string& transactionHash, const string& commandName, bool needsPluginNotification, void (*notificationHandler)(SQLite& _db, int64_t tableID), chrono::microseconds commitLockTimeout) noexcept
{
    // This handler only needs to exist in prepare so we scope it here to automatically unset
    // the handler function once we are done with prepare.
    {
        AutoScopeOnPrepare onPrepare(needsPluginNotification, _db, notificationHandler);

        // This will fail only if we can't acquire the commit lock respecting the command timeout, which
        // likely means our cluster health is not optimal. In this case, we want to roll back and
        // return false, which will make the caller return the appropriate exception.
        if (!_db.prepare(&commitID, &transactionHash, commitLockTimeout)) {
            _db.rollback(commandName);
            return false;
        }
    }

    // Check for any state other than leading and refuse.
    if (node.getState() != SQLiteNodeState::LEADING) {
        SINFO("No longer leading, rolling back.");
        _db.rollback(commandName);
        return false;
    }

    // If there's nothing to commit, we won't bother, but warn, as we should have noticed this already.
    if (_db.getUncommittedHash().empty()) {
        SWARN("Commit called with nothing to commit.");
        return true;
    }

    // Perform the actual commit, rollback if it fails.
    int errorCode = _db.commit(SQLiteNode::stateName(node.getState()), commandName);
    if (errorCode == SQLITE_BUSY_SNAPSHOT) {
        _db.rollback(commandName);
        return false;
    } else if (errorCode == SQLite::COMMIT_DISABLED) {
        SINFO("Commits currently disabled, rolling back.");
        _db.rollback(commandName);
        return false;
    }

    return true;
}

void SQLiteCore::rollback(const string& commandName)
{
    _db.rollback(commandName);
}
