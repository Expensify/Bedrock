#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
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
                cout << row[0] << ": " << maxCommitNum << endl;
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
        BedrockClusterTester tester;

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
        // TODO: We can just commit while not running.
        auto result = getMaxJournalCommit(tester.getTester(0));
        uint64_t leaderMaxCommit = result.first;
        string leaderMaxCommitJournal = result.second;
        result = getMaxJournalCommit(tester.getTester(1));
        uint64_t followerMaxCommit = result.first;

        // Make sure the follower got farther than the leader, by at least 2.
        ASSERT_GREATER_THAN(followerMaxCommit, leaderMaxCommit + 2);

        // Break leader.
        SData breakJournal("Query");
        // Oh. It's off... hmm.... 
        breakJournal["query"] = "INSERT INTO TEST VALUES(123456789, 'boop');";
        tester.getTester(0).executeWaitMultipleData({breakJournal});

        // Start the broken leader back up. We expect it will fail to synchronize.
        cout << "Starting" << endl;
        tester.getTester(0).startServer(false);
        cout << "Started" << endl;

        // And now send the check message to the whole cluster.
        SData checkFork("GetClusterCommitHash");
        checkFork["commit"] = to_string(leaderMaxCommit);
        checkFork["entireCluster"] = "true";
        auto results = tester.getTester(0).executeWaitMultipleData({breakJournal});
        cout << results[0].serialize() << endl;
    }
} __ForkCheckTest;
