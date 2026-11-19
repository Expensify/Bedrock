#include <future>
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>
#include <unistd.h>

struct HCTreeLeaderCommitTest : tpunit::TestFixture
{
    HCTreeLeaderCommitTest() : tpunit::TestFixture("HCTreeLeaderCommit", TEST(HCTreeLeaderCommitTest::parallelCommits))
    {
    }

    struct CommitPause
    {
        promise<void> atCommit;
        promise<void> resume;
        bool timedOut = false;

        static int trace(unsigned, void* context, void* statement, void*)
        {
            if (SIEquals(sqlite3_sql(static_cast<sqlite3_stmt*>(statement)), "COMMIT")) {
                // SQLite has assigned the CID, but the committing handle hasn't returned it to Bedrock yet.
                auto& pause = *static_cast<CommitPause*>(context);
                pause.atCommit.set_value();
                pause.timedOut = pause.resume.get_future().wait_for(5s) != future_status::ready;
            }
            return 0;
        }
    };

    void checkGap(bool conflict)
    {
        BedrockTester files({}, {}, 0, 0, 0, false);
        SQLite db(files.getArg("-db"), 1000, 1000, 0, 0, true);
        db.exclusiveLockDB();
        db.setHCTreeFollowerMode(false);
        db.exclusiveUnlockDB();
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.write("CREATE TABLE source (id INTEGER PRIMARY KEY, value INTEGER);"));
        ASSERT_TRUE(db.write("INSERT INTO source VALUES (1, 0), (2, 0), (3, 0);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        promise<void> firstPrepared, startCommit;
        CommitPause pause;
        uint64_t firstID = 0;
        auto first = async(launch::async, [&]() {
            SQLite writer(db);
            writer.beginTransaction();
            writer.read("SELECT value FROM source WHERE id = 1;");
            writer.write("UPDATE source SET value = 1 WHERE id = 2;");
            writer.prepare();
            firstPrepared.set_value();
            if (startCommit.get_future().wait_for(5s) != future_status::ready) {
                return SQLITE_ABORT;
            }
            sqlite3_trace_v2(writer.getDBHandle(), SQLITE_TRACE_PROFILE, CommitPause::trace, &pause);
            const int rc = writer.commit(firstID);
            sqlite3_trace_v2(writer.getDBHandle(), 0, nullptr, nullptr);
            if (rc != SQLITE_OK) {
                writer.rollback();
            }
            return rc;
        });
        ASSERT_TRUE(firstPrepared.get_future().wait_for(5s) == future_status::ready);
        if (conflict) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.write("UPDATE source SET value = 1 WHERE id = 1;"));
            ASSERT_TRUE(db.prepare(nullptr, nullptr, 1s));
            ASSERT_EQUAL(db.commit(), SQLITE_OK);
        }
        const auto before = db.getCommitState();
        db.popCommittedTransactions();

        // Prepare an independent write against the same snapshot, before either CID is allocated.
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.write("UPDATE source SET value = 1 WHERE id = 3;"));
        uint64_t secondID = 0;
        ASSERT_TRUE(db.prepare(&secondID, nullptr, 1s));
        EXPECT_EQUAL(secondID, 0ull);
        startCommit.set_value();
        ASSERT_TRUE(pause.atCommit.get_future().wait_for(5s) == future_status::ready);
        ASSERT_EQUAL(db.commit(secondID), SQLITE_OK);
        EXPECT_EQUAL(secondID, before.commitCount + 2);
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount);
        EXPECT_EQUAL(db.getCommittedHash(), before.hash);
        EXPECT_TRUE(db.popCommittedTransactions().empty());

        // Both a blocking transaction and the final stand-down flush must wait for the missing CID.
        promise<void> waiting;
        auto blocking = async(launch::async, [&]() {
            SQLite writer(db);
            waiting.set_value();
            if (conflict) {
                writer.setCommitEnabled(false);
                return true;
            }
            const bool began = writer.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
            writer.rollback();
            return began;
        });
        waiting.get_future().wait();
        // A waiting blocker must stop later committers, even though their reads can still proceed.
        SQLite late(db);
        bool admissionClosed = false;
        const auto deadline = chrono::steady_clock::now() + 2s;
        while (!admissionClosed && chrono::steady_clock::now() < deadline) {
            late.beginTransaction();
            admissionClosed = !late.prepare(nullptr, nullptr, 50ms);
            late.rollback();
            this_thread::yield();
        }
        EXPECT_TRUE(admissionClosed);
        EXPECT_TRUE(blocking.wait_for(0ms) == future_status::timeout);
        pause.resume.set_value();
        EXPECT_EQUAL(first.get(), conflict ? SQLITE_BUSY_SNAPSHOT : SQLITE_OK);
        EXPECT_FALSE(pause.timedOut);
        EXPECT_TRUE(blocking.get());
        ASSERT_EQUAL(firstID, before.commitCount + 1);
        const auto transactions = db.popCommittedTransactions();
        ASSERT_EQUAL(transactions.size(), 2ul);
        EXPECT_EQUAL(transactions.at(firstID).hash.empty(), conflict);
        EXPECT_EQUAL(transactions.at(firstID).query.empty(), conflict);
        EXPECT_EQUAL(db.getCommitCount(), secondID);
        EXPECT_EQUAL(db.getCommittedHash(), transactions.at(secondID).hash);
        EXPECT_TRUE(transactions.at(secondID).snapshot <= before.commitCount);
        EXPECT_EQUAL(db.read("SELECT snapshot FROM hct_journal WHERE cid = " + SQ(secondID)), to_string(transactions.at(secondID).snapshot));
        EXPECT_TRUE(db.popCommittedTransactions().empty());
        if (conflict) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.write("UPDATE source SET value = 2 WHERE id = 3;"));
            ASSERT_TRUE(db.prepare());
            EXPECT_TRUE(db.commit() == SQLite::COMMIT_DISABLED);
            db.rollback();
            EXPECT_TRUE(db.popCommittedTransactions().empty());
        }
    }

    void checkExclusiveSnapshot()
    {
        BedrockTester files({}, {}, 0, 0, 0, false);
        SQLite db(files.getArg("-db"), 1000, 1000, 0, 0, true);
        db.setHCTreeFollowerMode(false);
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.write("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER);"));
        ASSERT_TRUE(db.write("INSERT INTO t VALUES(1, 0);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        // This writer has modified the same row, but hasn't tried to commit yet.
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.write("UPDATE t SET value = 1 WHERE id = 1;"));
        promise<void> exclusiveReady, finish;
        auto blocking = async(launch::async, [&]() {
            SQLite writer(db);
            writer.setTimeout(2s);
            if (!writer.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE) || writer.read("SELECT value FROM t;") != "0" ||
                !writer.write("UPDATE t SET value = 2 WHERE id = 1;")) {
                return false;
            }
            SQLite nested(writer);
            if (nested.writeLocalUnreplicated("UPDATE t SET value = 3 WHERE id = 1;")) {
                return false;
            }
            exclusiveReady.set_value();
            return finish.get_future().wait_for(3s) == future_status::ready && writer.prepare() && writer.commit() == SQLITE_OK;
        });
        ASSERT_TRUE(exclusiveReady.get_future().wait_for(3s) == future_status::ready);
        SQLite reader(db);
        reader.setTimeout(1s);
        ASSERT_TRUE(reader.beginTransaction());
        EXPECT_EQUAL(reader.read("SELECT value FROM t;"), "0");
        reader.rollback();
        finish.set_value();
        ASSERT_TRUE(blocking.get());
        ASSERT_TRUE(db.prepare());
        EXPECT_EQUAL(db.commit(), SQLITE_BUSY_SNAPSHOT);
        db.rollback();
        EXPECT_EQUAL(db.read("SELECT value FROM t;"), "2");
    }

    void parallelCommits()
    {
        // Native journal mode is process-wide. Other unit fixtures may be running in parallel.
        if (!getenv("BEDROCK_TEST_PARALLEL_LEADER")) {
            ASSERT_EQUAL(system(("BEDROCK_TEST_PARALLEL_LEADER=1 /proc/" + to_string(getpid()) +
                "/exe -only HCTreeLeaderCommit -useNewOutput false").c_str()), 0);
            return;
        }
        SQLite::hctreeExperimentalMode = true;
        checkGap(false);
        checkGap(true);
        checkExclusiveSnapshot();
    }
} __HCTreeLeaderCommitTest;
