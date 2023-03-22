#include <sys/wait.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLite.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ForkCheckTest : tpunit::TestFixture {
    ForkCheckTest()
        : tpunit::TestFixture("ForkCheck", TEST(ForkCheckTest::test)) {}

    pair<uint64_t, string> getMaxJournalCommit(BedrockTester& tester) {
        SQResult journals;
        tester.readDB("SELECT name FROM sqlite_schema WHERE type ='table' AND name LIKE 'journal%';", journals);
        uint64_t maxJournalCommit = 0;
        string maxJournalTable;
        for (auto& row : journals.rows) {
            string maxID = tester.readDB("SELECT MAX(id) FROM " + row[0] + ";");
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

    static int offlineLookupCallback(void* ref, int numcol, char** colvals, char** colnames) {
        vector<string>& res = *(static_cast<vector<string>*>(ref));
        if (numcol) {
            res.push_back(colvals[0]);
        }

        return SQLITE_OK;
    }

    pair<uint64_t, string> getMaxJournalCommitOffline(BedrockTester& tester) {
        string filename = tester.getArg("-db");
        uint64_t maxJournalCommit = 0;
        string maxJournalTable;
        vector<string> results;
        sqlite3* db = nullptr;
        sqlite3_open_v2(filename.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
        char* errMsg = nullptr;
        sqlite3_exec(db, "SELECT name FROM sqlite_schema WHERE type ='table' AND name LIKE 'journal%';", &offlineLookupCallback, &results, &errMsg);
        if (errMsg) {
            cout << "Error updating db: " << errMsg << endl;
        }

        for (auto& journal : results) {
            results.clear();
            sqlite3_exec(db, string("SELECT MAX(id) FROM " + journal + ";").c_str(), &offlineLookupCallback, &results, &errMsg);
            if (results.size() && results[0] != "") {
                uint64_t maxCommitNum = stoull(results[0]);
                if (maxCommitNum > maxJournalCommit) {
                    maxJournalCommit = maxCommitNum;
                    maxJournalTable = journal;
                }
            }
        }
        sqlite3_close_v2(db);

        return make_pair(maxJournalCommit, maxJournalTable);
    }

    void test() {
        // Create a cluster, wait for it to come up.
        BedrockClusterTester tester(ClusterSize::FIVE_NODE_CLUSTER);

        // We'll tell the threads to stop when they're done.
        atomic<bool> stop(false);

        // We want to not spam a stopped leader.
        atomic<bool> leaderIsUp(true);

        // Just use a bunch of copies of the same command.
        SData spamCommand("idcollision");

        // In a vector.
        const vector<SData> commands(100, spamCommand);

        // Now create 9 threads spamming 100 commands at a time, each. 9 cause we have three nodes.
        vector<thread> threads;
        for (size_t i = 0; i < 9; i++) {
            threads.emplace_back([&tester, i, &commands, &stop, &leaderIsUp](){
                while (!stop) {
                    // Pick a tester, send, don't care about the result.
                    size_t testerNum = i % 3;
                    if (testerNum == 0 && !leaderIsUp) {
                        // If we're looking for leader and it's down, wait a second to avoid pegging the CPU.
                        sleep(1);
                    } else {
                        // If we're not leader or leader is up, spam away!
                        tester.getTester(testerNum).executeWaitMultipleData(commands);
                    }
                }
            });
        }

        // Let them spam for a second.
        sleep(1);

        // We can try and stop the leader.
        leaderIsUp = false;
        tester.getTester(0).stopServer();

        // Spam a few more commands and then we can stop.
        sleep(1);
        stop = true;
        for (auto& t : threads) {
            t.join();
        }

        // Break the journal on leader intentionally to fake a fork.
        auto result = getMaxJournalCommitOffline(tester.getTester(0));
        uint64_t leaderMaxCommit = result.first;
        string leaderMaxCommitJournal = result.second;
        result = getMaxJournalCommit(tester.getTester(1));
        uint64_t followerMaxCommit = result.first;

        // Make sure the follower got farther than the leader.
        ASSERT_GREATER_THAN(followerMaxCommit, leaderMaxCommit);

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
    }
} __ForkCheckTest;
