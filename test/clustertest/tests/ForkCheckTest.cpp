#include "test/lib/BedrockTester.h"
#include <sys/wait.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLite.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ForkCheckTest : tpunit::TestFixture
{
    ForkCheckTest()
        : tpunit::TestFixture("ForkCheck",
                              TEST(ForkCheckTest::forkAtShutDown))
    {
    }

    uint64_t getMaxJournalCommit(BedrockTester& tester, bool online = true)
    {
        const string maxID = tester.readDB("SELECT MAX(id) FROM journalEntries;", online);
        return maxID.empty() ? 0 : SToUInt64(maxID);
    }

    string getJournalTableForCommit(BedrockTester& tester, uint64_t commitID, bool online)
    {
        SQResult journals;
        tester.readDB("SELECT name FROM sqlite_schema WHERE type ='table' AND name LIKE 'journal%';", journals, online);
        for (auto& row : journals) {
            if (!tester.readDB("SELECT id FROM " + row[0] + " WHERE id = " + SQ(commitID) + ";", online).empty()) {
                return row[0];
            }
        }
        if (tester.readDB("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='hct_journal';", online) == "1" &&
            !tester.readDB("SELECT cid FROM hct_journal WHERE cid = " + SQ(commitID) + ";", online).empty()) {
            return "hct_journal";
        }
        return "";
    }

    vector<thread> createThreads(size_t num, BedrockClusterTester& tester, atomic<bool>& stop, atomic<bool>& leaderIsUp)
    {
        // Just use a bunch of copies of the same command.
        vector<thread> threads;
        for (size_t num = 0; num < 9; num++) {
            threads.emplace_back([&tester, num, &stop, &leaderIsUp](){
                const vector<SData> commands(100, SData("idcollision"));
                while (!stop) {
                    // Pick a tester, send, don't care about the result.
                    size_t testerNum = num % 5;
                    if (testerNum == 0 && !leaderIsUp) {
                        // If leader's off, don't use it.
                        testerNum = 1;
                    }
                    tester.getTester(testerNum).executeWaitMultipleData(commands);
                }
            });
        }

        return threads;
    }

    // This primary test here checks that a node that is forked will not be able to rejoin the cluster when reconnecting.
    // This is a reasonable test for a fork that happens at shutdown.
    void forkAtShutDown()
    {
        // Create a cluster, wait for it to come up.
        BedrockClusterTester tester(ClusterSize::FIVE_NODE_CLUSTER);

        // We'll tell the threads to stop when they're done.
        atomic<bool> stop(false);

        // We want to not spam a stopped leader.
        atomic<bool> leaderIsUp(true);

        // Now create 15 threads spamming 100 commands at a time, each. 15 because we have five nodes.
        vector<thread> threads = createThreads(15, tester, stop, leaderIsUp);

        // Let them spam for a second.
        sleep(1);

        // We can try and stop the leader.
        leaderIsUp = false;
        tester.getTester(0).stopServer();

        // Spam a few more commands so thar the follower is ahead of the stopped leader, and then we can stop.
        sleep(1);
        stop = true;
        for (auto& t : threads) {
            t.join();
        }

        // Break the journal on leader intentionally to fake a fork.
        const uint64_t leaderMaxCommit = getMaxJournalCommit(tester.getTester(0), false);
        const uint64_t followerMaxCommit = getMaxJournalCommit(tester.getTester(1));

        // Make sure the follower got farther than the leader.
        ASSERT_GREATER_THAN(followerMaxCommit, leaderMaxCommit);

        // A failed HC-Tree leader commit can leave a blank final entry, so corrupt
        // the most recent nonblank hash rather than assuming the highest CID has one.
        const string lastHashedID = tester.getTester(0).readDB("SELECT MAX(id) FROM journalEntries WHERE length(hash) > 0;", false);
        ASSERT_FALSE(lastHashedID.empty());
        const uint64_t corruptCommit = SToUInt64(lastHashedID);
        const string journalTable = getJournalTableForCommit(tester.getTester(0), corruptCommit, false);
        ASSERT_FALSE(journalTable.empty());

        // We need to release any DB that the tester is holding.
        tester.getTester(0).freeDB();

        // Break leader.
        {
            string filename = tester.getTester(0).getArg("-db");
            sqlite3* db = nullptr;
            ASSERT_EQUAL(sqlite3_open_v2(filename.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL), SQLITE_OK);
            if (journalTable == "hct_journal") {
                sqlite3_stmt* stmt = nullptr;
                ASSERT_EQUAL(sqlite3_prepare_v2(db, "SELECT query FROM hct_journal WHERE cid = ?", -1, &stmt, nullptr), SQLITE_OK);
                sqlite3_bind_int64(stmt, 1, corruptCommit);
                ASSERT_EQUAL(sqlite3_step(stmt), SQLITE_ROW);
                const char* data = static_cast<const char*>(sqlite3_column_blob(stmt, 0));
                const int size = sqlite3_column_bytes(stmt, 0);
                ASSERT_TRUE(data && size >= 74);
                string changed(data, size);
                changed[0] = changed[0] == '0' ? '1' : '0';
                ASSERT_EQUAL(sqlite3_finalize(stmt), SQLITE_OK);

                ASSERT_EQUAL(sqlite3_prepare_v2(db, "UPDATE hct_journal SET query = ? WHERE cid = ?", -1, &stmt, nullptr), SQLITE_OK);
                sqlite3_bind_blob(stmt, 1, changed.data(), changed.size(), SQLITE_TRANSIENT);
                sqlite3_bind_int64(stmt, 2, corruptCommit);
                ASSERT_EQUAL(sqlite3_step(stmt), SQLITE_DONE);
                ASSERT_EQUAL(sqlite3_changes(db), 1);
                ASSERT_EQUAL(sqlite3_finalize(stmt), SQLITE_OK);
            } else {
                string query = "UPDATE " + journalTable + " SET hash = 'abcdef123456' WHERE id = " + SQ(corruptCommit) + ";";
                ASSERT_EQUAL(sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr), SQLITE_OK);
                ASSERT_EQUAL(sqlite3_changes(db), 1);
            }
            ASSERT_EQUAL(sqlite3_close_v2(db), SQLITE_OK);
        }

        // Start the broken leader back up. We expect it will fail to synchronize.
        tester.getTester(0).startServer(false);

        // We expect it to die shortly.
        int status = 0;
        waitpid(tester.getTester(0).getPID(), &status, 0);

        // Should have gotten a signal when it died.
        ASSERT_TRUE(WIFSIGNALED(status));

        // And that signal should have been ABORT.
        ASSERT_EQUAL(SIGABRT, WTERMSIG(status));

        // We call stopServer on the forked leader because it crashed, but the cluster tester doesn't realize, so shutting down
        // normally will time out after a minute. Calling `stopServer` explicitly will clear the server PID, and we won't need
        // to wait for this timeout.
        tester.getTester(0).stopServer();
    }
} __ForkCheckTest;
