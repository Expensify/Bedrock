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

static void setPriority(BedrockTester& node, int priority)
{
    SData cmd("SetPriority");
    cmd["priority"] = to_string(priority);
    node.executeWaitVerifyContent(cmd, "200", true);
}

struct SetPriorityTest : tpunit::TestFixture
{
    SetPriorityTest()
        : tpunit::TestFixture("SetPriority",
                              TEST(SetPriorityTest::testValidation),
                              TEST(SetPriorityTest::testPriorityScenarios))
    {
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

    // All 6-node scenarios run in one method so we only spin up a single cluster.
    // The test framework hits port/PID-reuse issues after roughly 5 back-to-back
    // 6-node cluster startups, so spreading these scenarios across separate TEST()
    // methods causes the later clusters to time out at startup.
    void testPriorityScenarios()
    {
        BedrockClusterTester tester = BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER);
        BedrockTester& node0 = tester.getTester(0);
        BedrockTester& node1 = tester.getTester(1);
        BedrockTester& node2 = tester.getTester(2);
        BedrockTester& node3 = tester.getTester(3);
        BedrockTester& node5 = tester.getTester(5);

        // Initial roles. Priorities at startup: 100, 90, 80, 70, 60, 0 (node5 perma).
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node1.waitForState("FOLLOWING"));
        ASSERT_TRUE(node2.waitForState("FOLLOWING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));
        ASSERT_TRUE(tester.getTester(4).waitForState("FOLLOWING"));
        ASSERT_TRUE(node5.waitForState("FOLLOWING"));

        // --- Scenario 1: pure priority change, no re-election ---
        // node3 goes 70 -> 65. Still lower than the leader, no transition.
        setPriority(node3, 65);
        ASSERT_TRUE(node3.waitForStatusTerm("priority", "65"));
        ASSERT_TRUE(waitForPeerField(node0, "cluster_node_3", "priority", "65"));
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));

        // Restore node3 to 70 for subsequent scenarios.
        setPriority(node3, 70);
        ASSERT_TRUE(node3.waitForStatusTerm("priority", "70"));

        // --- Scenario 2: lower leader below a follower forces stand-down ---
        // node0 drops 100 -> 50. node1 (still 90) takes over.
        setPriority(node0, 50);
        ASSERT_TRUE(node1.waitForState("LEADING"));
        ASSERT_TRUE(node0.waitForState("FOLLOWING"));
        ASSERT_TRUE(node0.waitForStatusTerm("priority", "50"));

        // Restore node0 to 100; raising above the current leader (90) must
        // re-elect via the FOLLOWING -> SEARCHING path.
        setPriority(node0, 100);
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node1.waitForState("FOLLOWING"));

        // --- Scenario 3: raise a follower above the leader forces re-election ---
        // node2 jumps 80 -> 150. node2 takes over from node0 (100).
        setPriority(node2, 150);
        ASSERT_TRUE(node2.waitForState("LEADING"));
        ASSERT_TRUE(node0.waitForState("FOLLOWING"));
        ASSERT_TRUE(node2.waitForStatusTerm("priority", "150"));

        // Restore node2 to 80; node0 should retake leadership.
        setPriority(node2, 80);
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node2.waitForState("FOLLOWING"));

        // --- Scenario 4: demote leader to permafollower ---
        // node0 drops to 0; node1 (90) takes over. Peers see node0 with priority 0.
        setPriority(node0, 0);
        ASSERT_TRUE(node1.waitForState("LEADING"));
        ASSERT_TRUE(node0.waitForState("FOLLOWING"));
        ASSERT_TRUE(node0.waitForStatusTerm("priority", "0"));
        ASSERT_TRUE(waitForPeerField(node1, "cluster_node_0", "priority", "0"));

        // Restore node0 to 100 and reclaim leadership.
        setPriority(node0, 100);
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node1.waitForState("FOLLOWING"));
        // node5 was knocked into SUBSCRIBING by the prior re-election. Wait for
        // it to settle back into FOLLOWING — otherwise setPriority's FOLLOWING
        // branch won't trigger.
        ASSERT_TRUE(node5.waitForState("FOLLOWING"));

        // --- Scenario 5: promote the permafollower above everyone ---
        // node5 jumps 0 -> 200, becomes leader.
        setPriority(node5, 200);
        ASSERT_TRUE(node5.waitForState("LEADING"));
        ASSERT_TRUE(node0.waitForState("FOLLOWING"));
        ASSERT_TRUE(node5.waitForStatusTerm("priority", "200"));
    }
} __SetPriorityTest;
