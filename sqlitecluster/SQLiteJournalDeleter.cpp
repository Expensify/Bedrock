#include "libstuff/sqlite3.h"
#include "SQLiteJournalDeleter.h"
#include "sqlitecluster/SQLite.h"
#include "libstuff/SQResult.h"

SQLiteJournalDeleter::SQLiteJournalDeleter(SQLiteJournalDeleter::TableLimits limits, SQLite& db) :
  _commitCountHandle(db) {
    int result = sqlite3_open_v2(db.getFilename().c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
    if (result != SQLITE_OK) {
        STHROW("500 Couldn't open reader DB");
    }

    for (const auto& p : limits) {
        size_t limit = p.first;
        for (const auto& table : p.second) {
            _limits.push_back(make_pair(limit, table));
            SINFO("Limit " << limit << " for " << table);
        }
    }

    _deleteThread = thread(&SQLiteJournalDeleter::deleteEntries, this);
}

SQLiteJournalDeleter::~SQLiteJournalDeleter() {
    _stop = true;
    _deleteThread.join();
    SASSERT(!sqlite3_close(_db));
}

void SQLiteJournalDeleter::deleteEntries() {
    SInitialize("cleanup");
    SQResult results;
    while (!_stop) {
        results.clear();
        uint64_t commitCount = _commitCountHandle.getCommitCount();
        uint64_t limit = _limits[_nextTable].first;
        const string& table = _limits[_nextTable].second;

        try { 
            // If there's not enough rows to bother deleting, just skip.
            if (commitCount <= limit) {
                STHROW("Not enough rows");
            }

            uint64_t max = commitCount - limit;
            int success = SQuery(_db, "cleanup", "SELECT COUNT(*) FROM " + table + " WHERE id > " + SQ(max), results);
            if (success != SQLITE_OK) {
                SWARN("Error checking rows to delete from table: " << table);
                STHROW("Read error");
            }

            uint64_t toDelete = stoull(results[0][0]);
            if (toDelete) {
                SINFO("Deleting " << toDelete << " rows from " << table);
                success = SQuery(_db, "cleanup", "DELETE FROM " + table + " WHERE id > " + SQ(max), results);
                if (success == SQLITE_BUSY_SNAPSHOT) {
                    SINFO("Conflict deleting from table " << table);
                } else if (success != SQLITE_OK) {
                    SWARN("Error deleting from table " << table);
                    STHROW("Delete error");
                }
            }
        } catch (const SException& e) {
            // just go on to the next iteration without skipping incrementing the table.
        }

        // Move on to the next table.
        _nextTable = (_nextTable + 1) % _limits.size();

        // Sleep 100ms.
        usleep(100'000);
    }
}
