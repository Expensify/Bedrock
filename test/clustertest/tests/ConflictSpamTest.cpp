#include <libstuff/SData.h>
#include <libstuff/SRandom.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ConflictSpamTest : tpunit::TestFixture {
    ConflictSpamTest()
        : tpunit::TestFixture("ConflictSpam",
                              BEFORE_CLASS(ConflictSpamTest::setup),
                              AFTER_CLASS(ConflictSpamTest::teardown),
                              //TEST(ConflictSpamTest::slow),
                              TEST(ConflictSpamTest::spam)) { }

    /* What's a conflict spam test? The main point of this test is to make sure we have lots of conflicting commits
     * coming in to the whole cluster, so that we can make sure they all eventually get committed and replicated in a
     * sane way. This is supposed to be a "worst case scenario" test where we can verify that the database isn't
     * corrupted or anything else horrible happens even in less-than-ideal circumstances.
     */

    BedrockClusterTester* tester;
    atomic<int> cmdID;

    void setup() {
        cmdID.store(0);

        // Turn the settings for checkpointing way down so we can observe that both passive and full checkpoints
        // happen as expected.
        tester = new BedrockClusterTester();
        for (int i = 0; i < 3; i++) {
            BedrockTester& node = tester->getTester(i);
            SData controlCommand("SetCheckpointIntervals");
            controlCommand["passiveCheckpointPageMin"] = to_string(3);
            controlCommand["fullCheckpointPageMin"] = to_string(50);
            vector<SData> results = node.executeWaitMultipleData({controlCommand}, 1, true);

            // Verify we got a reasonable result.
            ASSERT_EQUAL(results.size(), 1);
            ASSERT_EQUAL(results[0].methodLine, "200 OK");
            ASSERT_EQUAL(results[0]["fullCheckpointPageMin"], to_string(25000));
            ASSERT_EQUAL(results[0]["passiveCheckpointPageMin"], to_string(2500));
        }
    }

    void teardown() {
        delete tester;
    }

    void slow()
    {
    }

    void spam()
    {
        recursive_mutex m;
        atomic<int> totalRequestFailures(0);

        // Set server 2 to a different version.
        tester->getTester(2).stopServer();
        tester->getTester(2).updateArgs({{"-versionOverride", "ABCDE"}});
        tester->getTester(2).startServer();

        // Let's spin up three threads, each spamming commands at one of our nodes (node number 2)
        list<thread> threads;
        atomic<int> commandNum(0);
        for (int i : {0, 1, 2, 3, 4}) {
            threads.emplace_back([this, i, &totalRequestFailures, &m, &commandNum](){
                BedrockTester& brtester = tester->getTester(2);

                // Let's make ourselves 20 commands to spam at each node.
                vector<SData> requests;
                int numCommands = 2000;
                for (int j = 0; j < numCommands; j++) {
                    SData query("get");
                    query["writeConsistency"] = "ASYNC";
                    query["num"] = to_string(++commandNum);
                    query["requestID"] = "rid" + query["num"] + to_string(SRandom::rand64());
                    requests.push_back(query);
                }

                // Ok, send them all!
                auto results = brtester.executeWaitMultipleData(requests);

                int failures = 0;
                for (auto row : results) {
                    if (SToInt(row.methodLine) != 200) {
                        cout << "[ConflictSpamTest] Node " << 2 << " Expected 200, got: " << SToInt(row.methodLine) << endl;
                        cout << "[ConflictSpamTest] " << row.content << endl;
                        failures++;
                    }
                    if (row.content.size() != 20000) {
                        cout << "Wrong response size! " << row.content.size() << ", command num? " << row["num"] << endl;
                        cout << row.serialize() << endl;
                    }
                }
                totalRequestFailures.fetch_add(failures);
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();
    }

} __ConflictSpamTest;
