#pragma once
#include <sqlitecluster/SQLite.h>

/* This class exists because when we read the test DB from a separate program, like BedrockTest, we don't
 * always have the commit count matching what we expect it to be, because the DB is getting updated by two
 * different executables. This class simply bypasses the check for commit count integrity.
 */
class TestSQLite : public SQLite {
  public:
    TestSQLite(const string& filename, int cacheSize, int autoCheckpoint, bool readOnly, int maxJournalSize);
    ~TestSQLite();
};
