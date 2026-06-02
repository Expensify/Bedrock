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
        BedrockTester& node4 = tester.getTester(4);
        BedrockTester& node5 = tester.getTester(5);

        // Initial roles. Priorities at startup: 100, 90, 80, 70, 60, 0 (node5 perma).
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node1.waitForState("FOLLOWING"));
        ASSERT_TRUE(node2.waitForState("FOLLOWING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));
        ASSERT_TRUE(node4.waitForState("FOLLOWING"));
        ASSERT_TRUE(node5.waitForState("FOLLOWING"));

        // Scenario 1: pure priority change, no re-election
        // node3 goes 70 -> 65. Still lower than the leader, no transition.
        setPriority(node3, 65);
        ASSERT_TRUE(node3.waitForStatusTerm("priority", "65"));
        ASSERT_TRUE(waitForPeerField(node0, "cluster_node_3", "priority", "65"));
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node3.waitForState("FOLLOWING"));

        // Restore node3 to 70 for subsequent scenarios.
        setPriority(node3, 70);
        ASSERT_TRUE(node3.waitForStatusTerm("priority", "70"));

        // Scenario 2: lower leader below a follower forces stand-down
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

        // Scenario 3: raise a follower above the leader forces re-election
        // Be sure that node2 is following (and not SUBSCRIBING) before changing
        // it's priority to remove flakiness
        ASSERT_TRUE(node2.waitForState("FOLLOWING"));
        // node2 jumps 80 -> 150. node2 takes over from node0 (100).
        setPriority(node2, 150);
        ASSERT_TRUE(node2.waitForState("LEADING"));
        ASSERT_TRUE(node0.waitForState("FOLLOWING"));
        ASSERT_TRUE(node2.waitForStatusTerm("priority", "150"));

        // Restore node2 to 80; node0 should retake leadership.
        // Wait for node2's view of node0 to reach FOLLOWING — setPriority
        // scans for a higher-priority FOLLOWING peer to force
        // stand-down, and that peer is node0 here. Without this wait, node0
        // may still appear SUBSCRIBING from node2's view right after the
        // scenario-3 election, leaving no peer to trigger stand-down.
        ASSERT_TRUE(waitForPeerField(node2, "cluster_node_0", "state", "FOLLOWING"));
        setPriority(node2, 80);
        ASSERT_TRUE(node0.waitForState("LEADING"));
        ASSERT_TRUE(node2.waitForState("FOLLOWING"));

        // Scenario 4: demote leader to permafollower
        // Wait for node0's view of node1 to be FOLLOWING before demoting so
        // node1 correctly takes over as leader. 
        ASSERT_TRUE(waitForPeerField(node0, "cluster_node_1", "state", "FOLLOWING"));
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

        // Scenario 5: promote the permafollower above everyone
        // Be sure that node2 is following (and not SUBSCRIBING) before changing 
        // it's priority to remove flakiness
        ASSERT_TRUE(node5.waitForState("FOLLOWING"));
        // node5 jumps 0 -> 200, becomes leader.
        setPriority(node5, 200);
        ASSERT_TRUE(node5.waitForState("LEADING"));
        ASSERT_TRUE(node0.waitForState("FOLLOWING"));
        ASSERT_TRUE(node5.waitForStatusTerm("priority", "200"));

        // Scenario 6: priority conflict rejection
        // Trying to set a priority that another peer already has must fail.
        // node1 is at 90 (still); try to make node3 also 90. Wait for node3's
        // view of node1 to actually reflect 90 — the conflict check reads from
        // the local peer list, so we need the value to be propagated first.
        ASSERT_TRUE(waitForPeerField(node3, "cluster_node_1", "priority", "90"));
        SData conflict("SetPriority");
        conflict["priority"] = "90";
        node3.executeWaitVerifyContent(conflict, "409", true);

        // node3's priority must not have changed.
        ASSERT_TRUE(node3.waitForStatusTerm("priority", "70"));

        // But two permafollowers (priority 0) are fine, so let's set multiple followers
        // to priority 0.
        setPriority(node3, 0);
        ASSERT_TRUE(node3.waitForStatusTerm("priority", "0"));
        setPriority(node4, 0);
        ASSERT_TRUE(node4.waitForStatusTerm("priority", "0"));

        // Scenario 7: quorum-preserving rejection
        // 6-node cluster needs at least 3 full peers for quorum.
        // Current full peers: node0 (100), node1 (90), node2 (80), node5 (200) — 4 total.
        // Demoting node2 leaves 3 full peers — exactly at the minimum, should succeed.
        setPriority(node2, 0);
        ASSERT_TRUE(node2.waitForStatusTerm("priority", "0"));

        // Now demoting any remaining full peer would drop us to 2 — below quorum.
        // node1's quorum check counts non-permafollower peers, so wait for node1's
        // view of nodes 2/3/4 to reflect their priority=0 (and thus permafollower
        // status) before issuing the demotion that must be rejected.
        ASSERT_TRUE(waitForPeerField(node1, "cluster_node_2", "priority", "0"));
        ASSERT_TRUE(waitForPeerField(node1, "cluster_node_3", "priority", "0"));
        ASSERT_TRUE(waitForPeerField(node1, "cluster_node_4", "priority", "0"));
        SData breakQuorum("SetPriority");
        breakQuorum["priority"] = "0";
        node1.executeWaitVerifyContent(breakQuorum, "409", true);

        // node1's priority must not have changed.
        ASSERT_TRUE(node1.waitForStatusTerm("priority", "90"));
    }
} __SetPriorityTest;
