#include <unistd.h>

#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

struct AfterCommitCallbackTempDBFile
{
    char filename[17] = "br_acc_dbXXXXXXX";
    AfterCommitCallbackTempDBFile()
    {
        int fd = mkstemp(filename);
        close(fd);
    }

    ~AfterCommitCallbackTempDBFile()
    {
        unlink(filename);
    }
};

struct AfterCommitCallbackTest : tpunit::TestFixture
{
    AfterCommitCallbackTest()
        : tpunit::TestFixture("AfterCommitCallback",
                              TEST(AfterCommitCallbackTest::firesOncePerCommit),
                              TEST(AfterCommitCallbackTest::doesNotFireOnRollback))
    {
    }

    void firesOncePerCommit()
    {
        AfterCommitCallbackTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, 1000, 1);

        atomic<int> callCount(0);
        db.registerAfterCommitCallback([&callCount]() {
            callCount++;
        });

        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testTable(id INTEGER PRIMARY KEY, value INTEGER);");
        db.prepare();
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        ASSERT_EQUAL(callCount.load(), 1);

        // Each subsequent commit adds exactly one more call.
        for (int i = 1; i <= 5; i++) {
            db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
            db.write("INSERT INTO testTable VALUES(" + SToStr(i) + ", " + SToStr(i) + ");");
            db.prepare();
            ASSERT_EQUAL(db.commit(), SQLITE_OK);
            ASSERT_EQUAL(callCount.load(), i + 1);
        }
    }

    void doesNotFireOnRollback()
    {
        AfterCommitCallbackTempDBFile dbFile;
        SQLite db(dbFile.filename, 1000, 1000, 1);

        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testTable(id INTEGER PRIMARY KEY, value INTEGER);");
        db.prepare();
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        // Register only now, so the setup commit above can't be mistaken for the write we roll back.
        atomic<int> callCount(0);
        db.registerAfterCommitCallback([&callCount]() {
            callCount++;
        });

        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("INSERT INTO testTable VALUES(1, 1);");
        db.rollback();
        ASSERT_EQUAL(callCount.load(), 0);

        // A transaction that is prepared and then rolled back also doesn't count.
        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("INSERT INTO testTable VALUES(2, 2);");
        db.prepare();
        db.rollback();
        ASSERT_EQUAL(callCount.load(), 0);
    }

} __AfterCommitCallbackTest;
