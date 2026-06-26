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

        // Allow commands to aport in prepare(), particularly, while waiting for the mutex.
        if (abortPtr) {
            _db.setAbortRef(*abortPtr);
        }

        bool prepareSuccess = false;
        try {
            // This will fail if we can't acquire the commit lock or the command has been aborted.
            prepareSuccess = _db.prepare(&commitID, &transactionHash, commitLockTimeout);
        } catch (const exception& e) {
            // _onPrepareHandler can potentially throw, depending on what it does. Particularly if the abortPtr's value has become true.
            // Since this function is noexcept, we convert this to a warning. The command will stil fail.
            SWARN("Exception " << e.what() << " caught in prepare()");
        }
        _db.clearAbortRef();

        if (!prepareSuccess) {
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
    if (errorCode) {
        if (errorCode == SQLITE_BUSY_SNAPSHOT || errorCode == SQLITE_INTERRUPT) {
            // No extra logging needed for expected cases.
        } else if (errorCode == SQLite::COMMIT_DISABLED) {
            SINFO("Commits currently disabled, rolling back.");
        } else {
            SWARN("Unexpected commit error: " << errorCode << ", rolling back.");
        }
        _db.rollback(commandName);
        return false;
    }

    return true;
}

void SQLiteCore::rollback(const string& commandName)
{
    _db.rollback(commandName);
}
