#include "../BedrockClusterTester.h"

struct MultipleLeaderSyncTest : tpunit::TestFixture {
    MultipleLeaderSyncTest()
        : tpunit::TestFixture("MultipleLeaderSyncTest",
                              TEST(MultipleLeaderSyncTest::test)
                             ) { }

    // Create a bunch of trivial write commands.
    void runTrivialWrites(int writeCount, BedrockTester& node) {
        int count = 0;
        while (count <= writeCount) {
            SData request;
            request.methodLine = "Query";
            if (count == 0) {
                request["query"] = "INSERT OR REPLACE INTO test (id, value) VALUES(12345, 1 );";
                node.executeWaitVerifyContent(request, "200");
            } else {
                request["query"] = "UPDATE test SET value=value + 1 WHERE id=12345;";
                request["connection"] = "forget";
                node.executeWaitVerifyContent(request, "202");
            }
            count++;
        }
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
        ASSERT_TRUE(node0.waitForStates({"LEADING", "MASTERING"}));
        ASSERT_TRUE(node1.waitForStates({"FOLLOWING", "SLAVING"}));
        ASSERT_TRUE(node2.waitForStates({"FOLLOWING", "SLAVING"}));
        ASSERT_TRUE(node3.waitForStates({"FOLLOWING", "SLAVING"}));
        ASSERT_TRUE(node4.waitForStates({"FOLLOWING", "SLAVING"}));

        // shut down primary leader, make sure secondary takes over
        tester.stopNode(0);
        ASSERT_TRUE(node1.waitForStates({"LEADING", "MASTERING"}));

        // move secondary leader enough commits ahead that primary leader can't catch up before our status tests
        runTrivialWrites(4000, node4);
        ASSERT_TRUE(node2.waitForCommit(4000, 100));
        ASSERT_TRUE(node3.waitForCommit(4000, 100));
        ASSERT_TRUE(node4.waitForCommit(4000, 100));

        // shut down secondary leader, make sure tertiary takes over
        tester.stopNode(1);
        ASSERT_TRUE(node2.waitForStates({"LEADING", "MASTERING"}));

        // create enough commits that secondary leader doesn't jump out of SYNCHRONIZING before our status tests
        runTrivialWrites(4000, node4);
        ASSERT_TRUE(node2.waitForCommit(8000, 100));
        ASSERT_TRUE(node3.waitForCommit(8000, 100));
        ASSERT_TRUE(node4.waitForCommit(8000, 100));

        // just a check for the ready state
        ASSERT_TRUE(node2.waitForStates({"LEADING", "MASTERING"}));
        ASSERT_TRUE(node3.waitForStates({"FOLLOWING", "SLAVING"}));
        ASSERT_TRUE(node4.waitForStates({"FOLLOWING", "SLAVING"}));

        // Bring leaders back up in reverse order, confirm priority, should go quickly to SYNCHRONIZING
        // There's a race in the below flow, to confirm primary master is up and syncing before secondary master gets synced up.
        tester.startNodeDontWait(1);
        ASSERT_TRUE(node1.waitForStatusTerm("Priority", "-1", 5'000'000, true));
        ASSERT_TRUE(node1.waitForStates({"SYNCHRONIZING"}, 10'000'000, true));
        tester.startNodeDontWait(0);
        ASSERT_TRUE(node0.waitForStatusTerm("Priority", "-1", 5'000'000, true));
        ASSERT_TRUE(node0.waitForStates({"SYNCHRONIZING"}, 10'000'000, true));

        // tertiary leader should still be MASTERING for a little while
        ASSERT_TRUE(node2.waitForStates({"LEADING", "MASTERING"}, 5'000'000, true));

        // secondary leader should catch up first and go MASTERING, wait up to 30s
        ASSERT_TRUE(node1.waitForStates({"LEADING", "MASTERING"}, 30'000'000, true));

        // when primary leader catches up it should go MASTERING, wait up to 30s
        ASSERT_TRUE(node0.waitForStates({"LEADING", "MASTERING"}, 30'000'000, true));

    }

} __MultipleLeaderSyncTest;
