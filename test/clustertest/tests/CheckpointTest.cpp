#include "../BedrockClusterTester.h"

struct CheckpointTest : tpunit::TestFixture {
    CheckpointTest()
        : tpunit::TestFixture("Checkpoint",
                              BEFORE_CLASS(CheckpointTest::setup),
                              AFTER_CLASS(CheckpointTest::teardown),
                              TEST(CheckpointTest::spam)) { }

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
            controlCommand["passiveCheckpointPageMin"] = to_string(50);
            controlCommand["fullCheckpointPageMin"] = to_string(75);
            // Send this twice, the second one will return the pre-existing values (the results of the first one).
            vector<SData> results = node.executeWaitMultipleData({controlCommand, controlCommand}, 1, true);

            // Verify we got a reasonable result.
            ASSERT_EQUAL(results.size(), 2);
            ASSERT_EQUAL(results[1].methodLine, "200 OK");
            ASSERT_EQUAL(results[1]["passiveCheckpointPageMin"], to_string(50));
            ASSERT_EQUAL(results[1]["fullCheckpointPageMin"], to_string(75));
        }
    }

    void teardown() {
        delete tester;
    }

    void spam()
    {
        recursive_mutex m;
        atomic<int> totalRequestFailures(0);

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &totalRequestFailures, &m](){
                BedrockTester& brtester = tester->getTester(i);

                // Let's make ourselves 200 commands to spam at each node.
                vector<SData> requests;
                int numCommands = 200;
                for (int j = 0; j < numCommands; j++) {
                    // Pick a command:
                    static vector<string> commands = {"idcollision", "slowquery", "slowprocessquery"};
                    const string commandName = commands[j % commands.size()];
                    SData command(commandName);
                    command["writeConsistency"] = "ASYNC";
                    int cmdNum = cmdID.fetch_add(1);
                    command["value"] = "sent-" + to_string(cmdNum);
                    if (commandName == "slowprocessquery") {
                        command["count"] = to_string(5000);
                    } else if (commandName == "slowquery") {
                        command["size"] = to_string(1000000);
                    }
                    requests.push_back(command);
                }

                // Ok, send them all!
                auto results = brtester.executeWaitMultipleData(requests);

                int failures = 0;
                for (auto row : results) {
                    if (SToInt(row.methodLine) != 200) {
                        cout << "Node " << i << " Expected 200, got: " << SToInt(row.methodLine) << endl;
                        cout << row.content << endl;
                        failures++;
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

        int fail = totalRequestFailures.load();
        if (fail > 0) {
            cout << "Total failures: " << fail << endl;
        }
        ASSERT_EQUAL(fail, 0);
    }

} __CheckpointTest;
