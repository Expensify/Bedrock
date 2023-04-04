
#include <sys/wait.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLite.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ForkedNodeApprovalTest : tpunit::TestFixture {
    ForkedNodeApprovalTest()
        : tpunit::TestFixture("ForkedNodeApproval", TEST(ForkedNodeApprovalTest::test)) {}

    pair<uint64_t, string> getMaxJournalCommit(BedrockTester& tester, bool online = true) {
        SQResult journals;
        tester.readDB("SELECT name FROM sqlite_schema WHERE type ='table' AND name LIKE 'journal%';", journals, online);
        uint64_t maxJournalCommit = 0;
        string maxJournalTable;
        for (auto& row : journals.rows) {
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

    void test() {
        // Create a cluster, wait for it to come up.
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);

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

        // Fetch the latest journal commits on leader and follower
        auto result = getMaxJournalCommit(tester.getTester(0), false);

        uint64_t leaderMaxCommit = result.first;
        string leaderMaxCommitJournal = result.second;
        result = getMaxJournalCommit(tester.getTester(1));
        uint64_t followerMaxCommit = result.first;

        // Make sure the follower got farther than the leader.
        ASSERT_GREATER_THAN(followerMaxCommit, leaderMaxCommit);

        // We need to release any DB that the tester is holding.
        tester.getTester(0).freeDB();
        tester.getTester(1).freeDB();

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

        // Stop the second follower.
        tester.getTester(2).stopServer();

        // Start the broken leader back up.
        tester.getTester(0).startServer(false);

        // We should not get a leader, the primary leader needs to synchronize, but can't because it's forked.
        // The secondary leader should go leading, but can't, because it only receives `abstain` responses to standup requests.
        // It's possible for the secondary leader to go leading once, but it should quickly fall out of leading when the fork is detected and primary leader reconnects.
        // After that, it should not go leading again, primary leader should abstain from participation.
        auto start = chrono::steady_clock::now();
        bool abstainDetected = false;
        while (true) {
            if (chrono::steady_clock::now() - start > 30s) {
                cout << "It's been 30 seconds." << endl;
                break;
            }
            SData command("Status");
            auto responseJson = tester.getTester(1).executeWaitMultipleData({command}, 1, true)[0].content;

            auto json = SParseJSONObject(responseJson);
            auto peers = SParseJSONArray(json["peerList"]);
            for (auto& peer : peers) {
                auto peerJSON = SParseJSONObject(peer);
                if (peerJSON["name"] == "cluster_node_0" && peerJSON["standupResponse"] == "ABSTAIN") {
                    abstainDetected = true;
                    break;
                }
            }
            if (abstainDetected) {
                break;
            }

            // try again.
            usleep(50'000);
        }

        ASSERT_TRUE(abstainDetected);

        // Ok, now we can start the second follower back up and secondary leader should be able to lead.
        tester.getTester(2).startServer(false);
        ASSERT_TRUE(tester.getTester(1).waitForState("LEADING"));
    }
} __ForkedNodeApprovalTest;
