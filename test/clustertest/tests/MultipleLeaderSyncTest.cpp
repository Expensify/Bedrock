#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct MultipleLeaderSyncTest : tpunit::TestFixture {
    MultipleLeaderSyncTest()
        : tpunit::TestFixture("MultipleLeaderSync",
                              TEST(MultipleLeaderSyncTest::test)
                             ) { }

    // Create a bunch of trivial write commands.
    void runTrivialWrites(int writeCount, BedrockTester& node) {
        int count = 0;

        SData genericRequest("Query");
        genericRequest["query"] = "UPDATE test SET value=value + 1 WHERE id=12345;";
        genericRequest["connection"] = "forget";
        genericRequest["writeConsistency"] = "ASYNC";
        vector<SData> genericRequests;
        for (int i = 0; i < 10; i++) {
            genericRequests.push_back(genericRequest);
        }

        while (count <= writeCount) {
            SData request;
            request.methodLine = "Query";
            if (count == 0) {
                request["query"] = "INSERT OR REPLACE INTO test (id, value) VALUES(12345, 1 );";
                node.executeWaitVerifyContent(request, "200");
                count += 1;
            } else {
                node.executeWaitMultipleData(genericRequests);
                count += 10;
            }
        }
    }

    bool waitForCommit(BedrockTester& node, uint64_t minCommitCount, uint64_t timeoutUS = 60'000'000) {
        uint64_t start = STimeNow();
        while (STimeNow() < start + timeoutUS) {
            try {
                string result = SParseJSONObject(node.executeWaitVerifyContent(SData("Status"), "200", true))["commitCount"];

                // if the value matches, return, otherwise wait
                if (SToUInt64(result) >= minCommitCount) {
                    return true;
                }
            } catch (...) {
                // Doesn't do anything, we'll fall through to the sleep and try again.
            }
            usleep(100'000);
        }
        return false;
    }

    void test() {
        // create a 5 node cluster
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::FIVE_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"});

        // get convenience handles for the cluster members
        BedrockTester& node0 = tester.getTester(0);
        BedrockTester& node1 = tester.getTester(1);
        BedrockTester& node2 = tester.getTester(2);
        BedrockTester& node3 = tester.getTester(3);
        BedrockTester& node4 = tester.getTester(4);

        // make sure the whole cluster is up
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node1.waitForState("FOLLOWING"));
        ASSERT_TRUE(node2.waitForState("FOLLOWING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));
        ASSERT_TRUE(node4.waitForState("FOLLOWING"));

        // shut down primary leader, make sure secondary takes over
        tester.stopNode(0);
        ASSERT_TRUE(node1.waitForState("LEADING"));

        // move secondary leader enough commits ahead that primary leader can't catch up before our status tests
        runTrivialWrites(4000, node1);
        ASSERT_TRUE(waitForCommit(node2, 4000));
        ASSERT_TRUE(waitForCommit(node3, 4000));
        ASSERT_TRUE(waitForCommit(node4, 4000));

        // shut down secondary leader, make sure tertiary takes over
        tester.stopNode(1);
        ASSERT_TRUE(node2.waitForState("LEADING"));

        // create enough commits that secondary leader doesn't jump out of SYNCHRONIZING before our status tests
        runTrivialWrites(4000, node2);
        ASSERT_TRUE(waitForCommit(node3, 8000));
        ASSERT_TRUE(waitForCommit(node4, 8000));

        // just a check for the ready state
        ASSERT_TRUE(node2.waitForState("LEADING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));
        ASSERT_TRUE(node4.waitForState("FOLLOWING"));

        // We want all of these states to happen.
        bool node2Leading = false;
        bool node1Synchronizing = false;
        bool node1Leading = false;
        bool node0Synchronizing = false;
        bool node0Leading = false;

        syslog(LOG_INFO, "bedrock TYLER");

        // Start up both servers.
        thread starter([&]() {
            tester.startNodeDontWait(1);
            tester.startNodeDontWait(0);
        });

        // Make sure we see node 2 leading.
        thread node2checker([&]() {
            uint64_t start = STimeNow();
            while (STimeNow() < start + 60'000'000) {
                try {
                    STable response = SParseJSONObject(node2.executeWaitVerifyContent(SData("Status"), "200", true));
                    if (response["state"] == "LEADING") {
                        node2Leading = true;
                        return;
                    }
                } catch (const SException& e) {}
                usleep(10'000);
            }
        });

        // Make sure we see node 1 synchronize and then lead.
        thread node1checker([&]() {
            uint64_t start = STimeNow();
            while (STimeNow() < start + 60'000'000) {
                try {
                    STable response = SParseJSONObject(node1.executeWaitVerifyContent(SData("Status"), "200", true));
                    if (response["state"] == "SYNCHRONIZING") {
                        node1Synchronizing = true;
                    }
                    if (response["state"] == "LEADING") {
                        node1Leading = true;
                    }
                    if (node1Synchronizing && node1Leading) {
                        return;
                    }
                } catch (const SException& e) {}
                usleep(10'000);
            }
        });

        // Make sure we see node 0 synchronize and then lead.
        thread node0checker([&]() {
            uint64_t start = STimeNow();
            while (STimeNow() < start + 60'000'000) {
                try {
                    STable response = SParseJSONObject(node0.executeWaitVerifyContent(SData("Status"), "200", true));
                    if (response["state"] == "SYNCHRONIZING") {
                        node0Synchronizing = true;
                    }
                    if (response["state"] == "LEADING") {
                        node0Leading = true;
                    }
                    if (node0Synchronizing && node0Leading) {
                        return;
                    }
                } catch (const SException& e) {}
                usleep(10'000);
            }
        });

        // Threads are done.
        starter.join();
        node0checker.join();
        node1checker.join();
        node2checker.join();

        // Verify we hit all of the cases we expected.
        ASSERT_TRUE(node2Leading);
        ASSERT_TRUE(node1Synchronizing);
        ASSERT_TRUE(node1Leading);
        ASSERT_TRUE(node0Synchronizing);
        ASSERT_TRUE(node0Leading);
    }
} __MultipleLeaderSyncTest;
