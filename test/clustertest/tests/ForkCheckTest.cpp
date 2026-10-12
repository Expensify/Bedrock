#include <test/lib/BedrockTester.h>
#include <sys/wait.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <libstuff/sqlite3.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ForkCheckTest : tpunit::TestFixture
{
    ForkCheckTest()
        : tpunit::TestFixture("ForkCheck",
                              TEST(ForkCheckTest::forkAtShutDown))
    {
    }

    vector<thread> createThreads(BedrockClusterTester& tester, atomic<bool>& stop, atomic<bool>& leaderIsUp)
    {
        // Just use a bunch of copies of the same command.
        vector<thread> threads;
        for (size_t client = 0; client < 9; client++) {
            threads.emplace_back([&tester, client, &stop, &leaderIsUp](){
                const vector<SData> commands(100, SData("idcollision"));
                while (!stop) {
                    // Pick a tester, send, don't care about the result.
                    size_t testerNum = client % 5;
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

        // Spam all five nodes with batches of 100 commands.
        vector<thread> threads = createThreads(tester, stop, leaderIsUp);

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

        const uint64_t followerMaxCommit = SToUInt64(tester.getTester(1).readDB("SELECT MAX(id) FROM journalEntries;"));

        // Inspect and corrupt the stopped leader through raw SQLite, without running Bedrock's startup initialization.
        {
            string filename = tester.getTester(0).getArg("-db");
            sqlite3* db = nullptr;
            ASSERT_EQUAL(sqlite3_open_v2(filename.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, NULL), SQLITE_OK);
            unique_ptr<sqlite3, decltype(& sqlite3_close)> connection(db, sqlite3_close);
            SQResult result;
            ASSERT_EQUAL(SQuery(db, "SELECT MAX(id) AS id FROM journalEntries;", result), SQLITE_OK);
            ASSERT_EQUAL(result.size(), 1ul);
            ASSERT_GREATER_THAN(followerMaxCommit, SToUInt64(result[0]["id"]));

            // A failed HC-Tree leader commit can leave a blank final entry, so corrupt the latest nonblank hash.
            ASSERT_EQUAL(SQuery(db, "SELECT MAX(id) AS id, hash FROM journalEntries WHERE length(hash) > 0;", result), SQLITE_OK);
            ASSERT_EQUAL(result.size(), 1ul);
            ASSERT_FALSE(result[0]["id"].empty());
            const uint64_t corruptCommit = SToUInt64(result[0]["id"]);
            if (BedrockTester::ENABLE_HCTREE) {
                sqlite3_stmt* stmt = nullptr;
                ASSERT_EQUAL(sqlite3_prepare_v2(db, "SELECT query FROM hct_journal WHERE cid = ?", -1, &stmt, nullptr), SQLITE_OK);
                sqlite3_bind_int64(stmt, 1, corruptCommit);
                ASSERT_EQUAL(sqlite3_step(stmt), SQLITE_ROW);
                const char* data = static_cast<const char*>(sqlite3_column_blob(stmt, 0));
                const int size = sqlite3_column_bytes(stmt, 0);
                ASSERT_TRUE(data && size >= static_cast<int>(result[0]["hash"].size() + 1));
                string changed(data, size);
                ASSERT_EQUAL(changed.substr(0, result[0]["hash"].size()), result[0]["hash"]);
                changed[0] = changed[0] == '0' ? '1' : '0';
                ASSERT_EQUAL(sqlite3_finalize(stmt), SQLITE_OK);

                ASSERT_EQUAL(sqlite3_prepare_v2(db, "UPDATE hct_journal SET query = ? WHERE cid = ?", -1, &stmt, nullptr), SQLITE_OK);
                sqlite3_bind_blob(stmt, 1, changed.data(), changed.size(), SQLITE_TRANSIENT);
                sqlite3_bind_int64(stmt, 2, corruptCommit);
                ASSERT_EQUAL(sqlite3_step(stmt), SQLITE_DONE);
                ASSERT_EQUAL(sqlite3_changes(db), 1);
                ASSERT_EQUAL(sqlite3_finalize(stmt), SQLITE_OK);
            } else {
                SQResult journals;
                ASSERT_EQUAL(SQuery(db, "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'journal%';", journals), SQLITE_OK);
                int changed = 0;
                for (const auto& row : journals) {
                    const string query = "UPDATE " + row["name"] + " SET hash = 'abcdef123456' WHERE id = " + SQ(corruptCommit) + ";";
                    ASSERT_EQUAL(sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr), SQLITE_OK);
                    changed += sqlite3_changes(db);
                }
                ASSERT_EQUAL(changed, 1);
            }
        }

        // Start the broken leader back up. We expect it will fail to synchronize.
        tester.getTester(0).startServerInBackground();

        // We expect it to die shortly.
        int status = 0;
        ASSERT_TRUE(tester.getTester(0).waitForExit(status));

        // Should have gotten a signal when it died.
        ASSERT_TRUE(WIFSIGNALED(status));

        // And that signal should have been ABORT.
        ASSERT_EQUAL(SIGABRT, WTERMSIG(status));
    }
} __ForkCheckTest;
