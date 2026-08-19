#include <unistd.h>

#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

struct WriteLocalUnreplicatedTempDBFile
{
    char filename[17] = "br_wlu_dbXXXXXXX";
    WriteLocalUnreplicatedTempDBFile()
    {
        int fd = mkstemp(filename);
        close(fd);
    }

    ~WriteLocalUnreplicatedTempDBFile()
    {
        unlink(filename);
    }
};

struct WriteLocalUnreplicatedTest : tpunit::TestFixture
{
    WriteLocalUnreplicatedTest()
        : tpunit::TestFixture("WriteLocalUnreplicated",
                              TEST(WriteLocalUnreplicatedTest::leavesCommitCountAndJournalUntouched))
    {
    }

    // The test DBs below are built with minJournalTables = 1, which creates exactly these three.
    int64_t countJournalRows(SQLite& db)
    {
        SQResult result;
        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        db.read("SELECT (SELECT COUNT(*) FROM journal) + (SELECT COUNT(*) FROM journal0000) + (SELECT COUNT(*) FROM journal0001);", result);
        db.rollback();
        return SToInt64(result[0][0]);
    }

    int64_t countRows(SQLite& db)
    {
        SQResult result;
        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        db.read("SELECT COUNT(*) FROM testTable;", result);
        db.rollback();
        return SToInt64(result[0][0]);
    }

    void leavesCommitCountAndJournalUntouched()
    {
        WriteLocalUnreplicatedTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, 1000, 1);

        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testTable(id INTEGER PRIMARY KEY, value INTEGER);");
        db.write("INSERT INTO testTable VALUES(1, 1);");
        db.prepare();
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        const uint64_t commitCountBefore = db.getCommitCount();
        const int64_t journalRowsBefore = countJournalRows(db);

        ASSERT_TRUE(db.writeLocalUnreplicated("DELETE FROM testTable WHERE id = 1;"));

        // The row is really gone from this node's database.
        ASSERT_EQUAL(countRows(db), 0);

        // But nothing about it was recorded as a commit, so there is nothing to ship to a peer.
        ASSERT_EQUAL(db.getCommitCount(), commitCountBefore);
        ASSERT_EQUAL(countJournalRows(db), journalRowsBefore);
    }

} __WriteLocalUnreplicatedTest;
