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
                              TEST(AfterCommitCallbackTest::firesOncePerCommit))
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

} __AfterCommitCallbackTest;
