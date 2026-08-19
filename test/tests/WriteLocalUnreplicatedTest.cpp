#include <thread>
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
                              TEST(WriteLocalUnreplicatedTest::leavesCommitCountAndJournalUntouched),
                              TEST(WriteLocalUnreplicatedTest::rollsBackOnFailedQuery),
                              TEST(WriteLocalUnreplicatedTest::survivesConcurrentCommits))
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

    void rollsBackOnFailedQuery()
    {
        WriteLocalUnreplicatedTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, 1000, 1);

        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testTable(id INTEGER PRIMARY KEY, value INTEGER);");
        db.prepare();
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        ASSERT_FALSE(db.writeLocalUnreplicated("DELETE FROM aTableThatDoesNotExist;"));

        // The failed call must not leave the handle stuck inside its transaction, so a normal commit still works.
        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("INSERT INTO testTable VALUES(1, 1);");
        db.prepare();
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        ASSERT_EQUAL(countRows(db), 1);

        // And a later unreplicated write on the same handle still works.
        ASSERT_TRUE(db.writeLocalUnreplicated("DELETE FROM testTable WHERE id = 1;"));
        ASSERT_EQUAL(countRows(db), 0);
    }

    void survivesConcurrentCommits()
    {
        WriteLocalUnreplicatedTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, 1000, 1);
        SQLite committer(db);

        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testTable(id INTEGER PRIMARY KEY, value INTEGER);");
        db.write("CREATE TABLE unreplicatedTable(id INTEGER PRIMARY KEY, value INTEGER);");
        db.prepare();
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        // Commit on one handle while the other runs unreplicated writes. A commit that lands mid-write makes the
        // COMMIT return SQLITE_BUSY_SNAPSHOT, which must come back as `false` rather than throwing or corrupting the
        // handle. Whether that race actually happens on any given run is not guaranteed, so this asserts the outcome
        // is always well formed rather than asserting a conflict occurred.
        atomic<bool> stop(false);
        thread committerThread([&]() {
            int id = 0;
            while (!stop) {
                committer.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
                committer.write("INSERT INTO testTable VALUES(" + SToStr(++id) + ", 1);");
                committer.prepare();
                if (committer.commit() != SQLITE_OK) {
                    committer.rollback();
                }
            }
        });

        int64_t succeeded = 0;
        for (int i = 1; i <= 200; i++) {
            if (db.writeLocalUnreplicated("INSERT INTO unreplicatedTable VALUES(" + SToStr(i) + ", 1);")) {
                succeeded++;
            }
        }

        stop = true;
        committerThread.join();

        // Every write that reported success is present, and no write that reported failure left anything behind.
        SQResult result;
        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        db.read("SELECT COUNT(*) FROM unreplicatedTable;", result);
        db.rollback();
        ASSERT_EQUAL(SToInt64(result[0][0]), succeeded);
    }
} __WriteLocalUnreplicatedTest;
