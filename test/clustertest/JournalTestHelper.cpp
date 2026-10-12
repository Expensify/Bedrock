#include <test/clustertest/JournalTestHelper.h>
#include <sqlitecluster/SQLite.h>

namespace {
void commitLegacy(SQLite& db, const string& query)
{
    const optional<string> guid = query.empty() ? optional<string>("") : nullopt;
    if (!db.beginTransaction() || (!query.empty() && !db.write(query)) ||
        !db.prepare(nullptr, nullptr, chrono::hours(24), nullptr, guid) || db.commit() != SQLITE_OK) {
        STHROW("500 Failed to seed legacy journal: " + db.getLastError());
    }
}
}

JournalTestHelper::LegacyHistory JournalTestHelper::seedLegacyHistory(const string& filename, bool hctree, const string& table, int rows, bool blankTail)
{
    if (SQLite::hctreeExperimentalMode) {
        STHROW("500 Legacy journal setup requires experimental mode to be disabled in the test process");
    }
    SQLite db(filename, 1000, 25000, 1, 0, hctree);
    commitLegacy(db, "CREATE TABLE " + table + "(id INTEGER PRIMARY KEY, value TEXT);");
    for (int i = 0; i < rows; ++i) {
        commitLegacy(db, "INSERT INTO " + table + " VALUES(" + SQ(i) + ", 'legacy');");
    }
    if (blankTail) {
        commitLegacy(db, "");
    }
    LegacyHistory result{db.getCommitCount(), db.getCommittedHash(), {}};
    if (!db.read("SELECT id, hex(query) AS query, hash FROM journalEntries ORDER BY id;", result.entries)) {
        STHROW("500 Failed to read seeded journal: " + db.getLastError());
    }
    return result;
}
