#include <unistd.h>

#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

struct JournalDeleterTempDBFile
{
    char filename[16] = "br_jd_dbXXXXXXX";
    JournalDeleterTempDBFile()
    {
        int fd = mkstemp(filename);
        close(fd);
    }

    ~JournalDeleterTempDBFile()
    {
        unlink(filename);
    }
};

struct JournalDeleterTest : tpunit::TestFixture
{
    JournalDeleterTest()
        : tpunit::TestFixture("JournalDeleter",
                              TEST(JournalDeleterTest::prepareLeavesTheJournalUntouched),
                              TEST(JournalDeleterTest::trimRemovesEntriesOlderThanTheMax),
                              TEST(JournalDeleterTest::trimKeepsEverythingWhenUnderTheMax),
                              TEST(JournalDeleterTest::trimHonoursTheBatchSize))
    {
    }

    // Small enough that a handful of commits is already over the limit.
    static const int maxJournalSize = 5;

    // Commits `count` transactions, including the one that creates the table.
    void commitTransactions(SQLite& db, int count)
    {
        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testTable(id INTEGER PRIMARY KEY, value INTEGER);");
        db.prepare();
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        for (int i = 2; i <= count; i++) {
            db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
            db.write("INSERT INTO testTable VALUES(" + SToStr(i) + ", 1);");
            db.prepare();
            ASSERT_EQUAL(db.commit(), SQLITE_OK);
        }
        ASSERT_EQUAL(db.getCommitCount(), (uint64_t) count);
    }

    int64_t countJournalRows(SQLite& db, const string& where = "1")
    {
        SQResult result;
        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        db.read("SELECT COUNT(*) FROM journalEntries WHERE " + where + ";", result);
        db.rollback();
        return SToInt64(result[0][0]);
    }

    // Deletes everything the deleter thread would eventually delete.
    void trimEverything(SQLite& db)
    {
        for (size_t round = 0; round < 10; round++) {
            for (size_t table = 0; table < db.getJournalTableCount(); table++) {
                ASSERT_TRUE(db.trimJournalTable(table, 1000));
            }
        }
    }

    void prepareLeavesTheJournalUntouched()
    {
        JournalDeleterTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, maxJournalSize, 1);
        commitTransactions(db, 20);

        // Committing is well past `maxJournalSize`, and every commit is still journaled: trimming is somebody else's
        // job now.
        ASSERT_EQUAL(countJournalRows(db), 20);
    }

    void trimRemovesEntriesOlderThanTheMax()
    {
        JournalDeleterTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, maxJournalSize, 1);
        commitTransactions(db, 30);

        const uint64_t oldestCommitToKeep = db.getCommitCount() - maxJournalSize;
        trimEverything(db);

        ASSERT_EQUAL(countJournalRows(db, "id < " + SQ(oldestCommitToKeep)), 0);
        ASSERT_EQUAL(countJournalRows(db), maxJournalSize + 1);
    }

    void trimKeepsEverythingWhenUnderTheMax()
    {
        JournalDeleterTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, 1000, 1);
        commitTransactions(db, 10);

        trimEverything(db);
        ASSERT_EQUAL(countJournalRows(db), 10);
    }

    void trimHonoursTheBatchSize()
    {
        JournalDeleterTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, maxJournalSize, 1);
        commitTransactions(db, 30);

        // 24 rows are old enough to delete, spread evenly over the journal tables, so every table has some.
        const uint64_t oldestCommitToKeep = db.getCommitCount() - maxJournalSize;
        const string oldRows = "id < " + SQ(oldestCommitToKeep);
        const int64_t oldRowsBefore = countJournalRows(db, oldRows);

        for (size_t table = 0; table < db.getJournalTableCount(); table++) {
            ASSERT_TRUE(db.trimJournalTable(table, 1));
        }

        ASSERT_EQUAL(countJournalRows(db, oldRows), oldRowsBefore - (int64_t) db.getJournalTableCount());
    }
} __JournalDeleterTest;
