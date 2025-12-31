#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct PermafollowerTest : tpunit::TestFixture
{
    PermafollowerTest()
        : tpunit::TestFixture("Permafollower",
                              TEST(PermafollowerTest::test)
        )
    {
    }

    void test()
    {
        // create a 6 node cluster
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"});

        // get convenience handles for the cluster members
        BedrockTester& node0 = tester.getTester(0);
        BedrockTester& node1 = tester.getTester(1);
        BedrockTester& node2 = tester.getTester(2);
        BedrockTester& node3 = tester.getTester(3);
        BedrockTester& node4 = tester.getTester(4);
        BedrockTester& node5 = tester.getTester(5);

        // make sure the whole cluster is up
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node1.waitForState("FOLLOWING"));
        ASSERT_TRUE(node2.waitForState("FOLLOWING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));
        ASSERT_TRUE(node4.waitForState("FOLLOWING"));
        ASSERT_TRUE(node5.waitForState("FOLLOWING"));

        // Confirm permafollower priority is correct
        ASSERT_TRUE(node5.waitForStatusTerm("Priority", "0", 5'000'000));

        // Shut down less than half the full peers
        tester.stopNode(1);
        tester.stopNode(2);

        // Now 4 out of 6 nodes (1 a permafollower) Do a full quorum commit
        SData request;
        request.methodLine = "Query";
        request["query"] = "INSERT OR REPLACE INTO test (id, value) VALUES(12345, 1 );";
        request["writeConsistency"] = "QUORUM";
        node4.executeWaitVerifyContent(request, "200");

        // Shut down permafollower
        tester.stopNode(5);

        // Now 3 out of 6 nodes Do another full quorum commit
        node4.executeWaitVerifyContent(request, "200");
    }
} __PermafollowerTest;
