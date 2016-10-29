#include "TestSQLite.h"

TestSQLite::TestSQLite(const string& filename, int cacheSize, int autoCheckpoint, bool readOnly, int maxJournalSize) :
    SQLite(filename, cacheSize, autoCheckpoint, readOnly, maxJournalSize)
{
}

TestSQLite::~TestSQLite()
{
    SQResult result;
    SASSERT(SQuery( _db, "verifying commit count", "SELECT MAX(id) FROM journal", result));
    _commitCount = SToUInt64(result[0][0]);
}
