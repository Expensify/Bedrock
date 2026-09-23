#include <libstuff/AutoScopeOnPrepare.h>
#include <libstuff/libstuff.h>
#include "SQLiteCore.h"
#include "SQLite.h"
#include "SQLiteNode.h"
#include "libstuff/sqlite3.h"

SQLiteCore::SQLiteCore(SQLite& db) : _db(db)
{
}

bool SQLiteCore::commit(const SQLiteNode& node, uint64_t& commitID, string& transactionHash, const string& commandName, bool needsPluginNotification, void (*notificationHandler)(SQLite& _db, int64_t tableID), chrono::microseconds commitLockTimeout, atomic<bool>* abortPtr) noexcept
{
    // This handler only needs to exist in prepare so we scope it here to automatically unset
    // the handler function once we are done with prepare.
    {
        AutoScopeOnPrepare onPrepare(needsPluginNotification, _db, notificationHandler);

        // This will fail only if we can't acquire the commit lock respecting the command timeout, or the command is aborted while waiting for it.
        // In this case, we want to roll back and return false, which will make the caller return the appropriate exception.
        if (!_db.prepare(&commitID, &transactionHash, commitLockTimeout, abortPtr)) {
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

    // Perform the actual commit, rollback if it fails.
    const uint64_t previousCommitCount = _db.getCommitCount();
    int errorCode = _db.commit(SQLiteNode::stateName(node.getState()), commandName);
    if (errorCode) {
        if (errorCode == SQLITE_BUSY_SNAPSHOT) {
            // No extra logging needed for expected case.
        } else if (errorCode == SQLite::COMMIT_DISABLED) {
            SINFO("Commits currently disabled, rolling back.");
        } else {
            SWARN("Unexpected commit error: " << errorCode << ", rolling back.");
        }
        _db.rollback(commandName);
        if (_db.getCommitCount() > previousCommitCount) {
            // A failed HC-Tree leader commit can still allocate a CID and write
            // an empty journal entry, which the followers must receive.
            node.notifyCommit();
        }
        return false;
    }

    return true;
}

void SQLiteCore::rollback(const string& commandName)
{
    _db.rollback(commandName);
}
