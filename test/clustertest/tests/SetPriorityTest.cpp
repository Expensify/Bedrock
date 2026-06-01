#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

// Returns the peer object (parsed STable) named `peerName` from the Status
// response of `node`, or an empty STable if not found within the timeout.
static STable findPeer(BedrockTester& node, const string& peerName, uint64_t timeoutUS = 10'000'000)
{
    uint64_t start = STimeNow();
    while (STimeNow() < start + timeoutUS) {
        try {
            STable status = SParseJSONObject(node.executeWaitVerifyContent(SData("Status"), "200", true));
            list<string> peers = SParseJSONArray(status["peerList"]);
            for (const string& raw : peers) {
                STable peer = SParseJSONObject(raw);
                if (peer["name"] == peerName) {
                    return peer;
                }
            }
        } catch (...) {
            // Try again until we time out.
        }
        usleep(100'000);
    }
    return {};
}

// Polls `node`'s peer list until the named peer matches the expected key/value, or times out.
static bool waitForPeerField(BedrockTester& node, const string& peerName, const string& field, const string& expected, uint64_t timeoutUS = 10'000'000)
{
    uint64_t start = STimeNow();
    while (STimeNow() < start + timeoutUS) {
        STable peer = findPeer(node, peerName, 1'000'000);
        if (peer[field] == expected) {
            return true;
        }
        usleep(100'000);
    }
    return false;
}

struct SetPriorityTest : tpunit::TestFixture
{
    SetPriorityTest()
        : tpunit::TestFixture("SetPriority",
                              TEST(SetPriorityTest::testValidation),
                              TEST(SetPriorityTest::testPriorityChangePropagates),
                              TEST(SetPriorityTest::testLowerLeaderForcesStandDown),
                              TEST(SetPriorityTest::testRaiseFollowerForcesElection),
                              TEST(SetPriorityTest::testDemoteLeaderToPermafollower),
                              TEST(SetPriorityTest::testPromotePermafollower))
    {
    }

    // Confirms the cluster has settled into the expected initial roles for a 6-node cluster.
    void waitForInitialCluster(BedrockClusterTester& tester)
    {
        ASSERT_TRUE(tester.getTester(0).waitForState("LEADING"));
        ASSERT_TRUE(tester.getTester(1).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester.getTester(2).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester.getTester(3).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester.getTester(4).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester.getTester(5).waitForState("FOLLOWING"));
    }

    void testValidation()
    {
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::THREE_NODE_CLUSTER);
        ASSERT_TRUE(tester.getTester(0).waitForState("LEADING"));
        BedrockTester& follower = tester.getTester(1);

        // Missing priority header.
        SData missing("SetPriority");
        follower.executeWaitVerifyContent(missing, "400 Missing priority", true);

        // Priority 1 is reserved as the shutdown sentinel.
        SData reserved("SetPriority");
        reserved["priority"] = "1";
        follower.executeWaitVerifyContent(reserved, "400 Invalid priority", true);

        // Negative priority is invalid.
        SData negative("SetPriority");
        negative["priority"] = "-5";
        follower.executeWaitVerifyContent(negative, "400 Invalid priority", true);
    }

    void testPriorityChangePropagates()
    {
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER);
        waitForInitialCluster(tester);

        // Pick a follower whose new priority still leaves the leader in place.
        // node3 starts at 70; lower it to 65 — no re-election should follow.
        BedrockTester& node3 = tester.getTester(3);
        SData setPriority("SetPriority");
        setPriority["priority"] = "65";
        node3.executeWaitVerifyContent(setPriority, "200", true);

        // node3 reflects the new priority in its own status.
        ASSERT_TRUE(node3.waitForStatusTerm("priority", "65"));

        // node0 (leader) sees node3 with the new priority.
        ASSERT_TRUE(waitForPeerField(tester.getTester(0), "cluster_node_3", "priority", "65"));

        // Cluster did not re-elect.
        ASSERT_TRUE(tester.getTester(0).waitForState("LEADING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));
    }

    void testLowerLeaderForcesStandDown()
    {
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER);
        waitForInitialCluster(tester);

        // Leader is node0 at 100, node1 is highest follower at 90.
        // Drop node0 to 50 — node1 should take over.
        BedrockTester& leader = tester.getTester(0);
        SData setPriority("SetPriority");
        setPriority["priority"] = "50";
        leader.executeWaitVerifyContent(setPriority, "200", true);

        ASSERT_TRUE(tester.getTester(1).waitForState("LEADING"));
        ASSERT_TRUE(leader.waitForState("FOLLOWING"));
        ASSERT_TRUE(leader.waitForStatusTerm("priority", "50"));
    }

    void testRaiseFollowerForcesElection()
    {
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER);
        waitForInitialCluster(tester);

        // node2 starts at 80. Raise it to 150 (above node0's 100) — node2 should take over.
        BedrockTester& node2 = tester.getTester(2);
        SData setPriority("SetPriority");
        setPriority["priority"] = "150";
        node2.executeWaitVerifyContent(setPriority, "200", true);

        ASSERT_TRUE(node2.waitForState("LEADING"));
        ASSERT_TRUE(tester.getTester(0).waitForState("FOLLOWING"));
        ASSERT_TRUE(node2.waitForStatusTerm("priority", "150"));
    }

    void testDemoteLeaderToPermafollower()
    {
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER);
        waitForInitialCluster(tester);

        // Drop leader (node0) to permafollower (priority 0).
        BedrockTester& leader = tester.getTester(0);
        SData setPriority("SetPriority");
        setPriority["priority"] = "0";
        leader.executeWaitVerifyContent(setPriority, "200", true);

        // node1 (highest remaining non-permafollower at 90) should take over.
        ASSERT_TRUE(tester.getTester(1).waitForState("LEADING"));
        ASSERT_TRUE(leader.waitForState("FOLLOWING"));
        ASSERT_TRUE(leader.waitForStatusTerm("priority", "0"));

        // The new leader's view of node0 should reflect priority 0.
        ASSERT_TRUE(waitForPeerField(tester.getTester(1), "cluster_node_0", "priority", "0"));
    }

    void testPromotePermafollower()
    {
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER);
        waitForInitialCluster(tester);

        // node5 is the permafollower (priority 0). Promote it above everyone.
        BedrockTester& perma = tester.getTester(5);
        SData setPriority("SetPriority");
        setPriority["priority"] = "200";
        perma.executeWaitVerifyContent(setPriority, "200", true);

        // node5 should now lead the cluster.
        ASSERT_TRUE(perma.waitForState("LEADING"));
        ASSERT_TRUE(tester.getTester(0).waitForState("FOLLOWING"));
        ASSERT_TRUE(perma.waitForStatusTerm("priority", "200"));
    }
} __SetPriorityTest;
