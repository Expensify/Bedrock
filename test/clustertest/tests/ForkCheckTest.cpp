#include "test/lib/BedrockTester.h"
#include <sys/wait.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLite.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ForkCheckTest : tpunit::TestFixture {
    ForkCheckTest()
        : tpunit::TestFixture("ForkCheck",
          TEST(ForkCheckTest::forkAtShutDown)) {}

    pair<uint64_t, string> getMaxJournalCommit(BedrockTester& tester, bool online = true) {
        SQResult journals;
        tester.readDB("SELECT name FROM sqlite_schema WHERE type ='table' AND name LIKE 'journal%';", journals, online);
        uint64_t maxJournalCommit = 0;
        string maxJournalTable;
        for (auto& row : journals) {
            string maxID = tester.readDB("SELECT MAX(id) FROM " + row[0] + ";", online);
            try {
                uint64_t maxCommitNum = stoull(maxID);
                if (maxCommitNum > maxJournalCommit) {
                    maxJournalCommit = maxCommitNum;
                    maxJournalTable = row[0];
                }
            } catch (const invalid_argument& e) {
                // do nothing, skip this journal with no entries.
                continue;
            }
        }
        return make_pair(maxJournalCommit, maxJournalTable);
    }

    vector<thread> createThreads(size_t num, BedrockClusterTester& tester, atomic<bool>& stop, atomic<bool>& leaderIsUp) {
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
    void forkAtShutDown() {
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
        auto result = getMaxJournalCommit(tester.getTester(0), false);

        uint64_t leaderMaxCommit = result.first;
        string leaderMaxCommitJournal = result.second;
        result = getMaxJournalCommit(tester.getTester(1));
        uint64_t followerMaxCommit = result.first;

        // Make sure the follower got farther than the leader.
        ASSERT_GREATER_THAN(followerMaxCommit, leaderMaxCommit);

        // We need to release any DB that the tester is holding.
        tester.getTester(0).freeDB();

        // Break leader.
        {
            string filename = tester.getTester(0).getArg("-db");
            string query = "UPDATE " + leaderMaxCommitJournal + " SET hash = 'abcdef123456' WHERE id = " + to_string(leaderMaxCommit) + ";";

            sqlite3* db = nullptr;
            sqlite3_open_v2(filename.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
            char* errMsg = nullptr;
            sqlite3_exec(db, query.c_str(), 0, 0, &errMsg);
            if (errMsg) {
                cout << "Error updating db: " << errMsg << endl;
            }
            sqlite3_close_v2(db);
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
