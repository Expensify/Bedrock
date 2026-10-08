#include <thread>
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

struct WriteTransactionDrainTest : tpunit::TestFixture
{
    WriteTransactionDrainTest()
        : tpunit::TestFixture("WriteTransactionDrain",
                              TEST(WriteTransactionDrainTest::drainsNonblockingWriter),
                              TEST(WriteTransactionDrainTest::drainsBlockingWriterBeforeFirstWrite))
    {
    }

    void checkDrain(bool blocking)
    {
        char filename[] = "br_drain_dbXXXXXX";
        int fd = mkstemp(filename);
        close(fd);
        {
            SQLite writer(filename, 1000, 1000, 0, 0, BedrockTester::ENABLE_HCTREE);
            SQLite blocker(writer);
            ASSERT_TRUE(writer.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE));
            ASSERT_TRUE(writer.write("CREATE TABLE test(id INTEGER PRIMARY KEY);"));
            ASSERT_TRUE(writer.prepare());
            ASSERT_EQUAL(writer.commit(), SQLITE_OK);

            ASSERT_TRUE(writer.beginTransaction(blocking ? SQLite::TRANSACTION_TYPE::EXCLUSIVE : SQLite::TRANSACTION_TYPE::SHARED));
            if (!blocking) {
                ASSERT_TRUE(writer.write("INSERT INTO test VALUES(1);"));
            }

            atomic<bool> started(false);
            atomic<bool> acquired(false);
            atomic<bool> release(false);
            thread waiter([&]() {
                started = true;
                blocker.exclusiveLockDB();
                acquired = true;
                while (!release) {
                    usleep(1'000);
                }
                blocker.exclusiveUnlockDB();
            });
            while (!started) {
                usleep(1'000);
            }
            usleep(50'000);
            bool blockedBeforeCommit = !acquired;
            const bool writeSucceeded = writer.write("INSERT INTO test VALUES(2);");
            const bool prepareSucceeded = writeSucceeded && writer.prepare();
            const int commitResult = prepareSucceeded ? writer.commit() : SQLITE_ERROR;
            if (commitResult != SQLITE_OK) {
                writer.rollback();
            }

            uint64_t deadline = STimeNow() + 5'000'000;
            while (!acquired && STimeNow() < deadline) {
                usleep(1'000);
            }
            bool acquiredAfterCommit = acquired;
            release = true;
            waiter.join();
            ASSERT_TRUE(blockedBeforeCommit);
            ASSERT_TRUE(acquiredAfterCommit);
            ASSERT_TRUE(writeSucceeded);
            ASSERT_TRUE(prepareSucceeded);
            ASSERT_EQUAL(commitResult, SQLITE_OK);
        }
        unlink(filename);
        unlink((string(filename) + "-pagemap").c_str());
        unlink((string(filename) + "-log-0").c_str());
    }

    void drainsNonblockingWriter()
    {
        checkDrain(false);
    }

    void drainsBlockingWriterBeforeFirstWrite()
    {
        checkDrain(true);
    }
} __WriteTransactionDrainTest;
