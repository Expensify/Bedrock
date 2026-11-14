#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct AfterCommitCallbackClusterTest : tpunit::TestFixture
{
    AfterCommitCallbackClusterTest()
        : tpunit::TestFixture("AfterCommitCallbackCluster",
                              BEFORE_CLASS(AfterCommitCallbackClusterTest::setup),
                              AFTER_CLASS(AfterCommitCallbackClusterTest::teardown),
                              TEST(AfterCommitCallbackClusterTest::firesOnFollowerReplication))
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    uint64_t getAfterCommitCount(BedrockTester& node)
    {
        SData command("getaftercommitcount");
        return SToUInt64(node.executeWaitVerifyContentTable(command)["afterCommitCount"]);
    }

    void firesOnFollowerReplication()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));

        const uint64_t leaderCountBefore = getAfterCommitCount(leader);
        const uint64_t followerCountBefore = getAfterCommitCount(follower);

        // Write on the leader. The follower never runs this command, it only applies the replicated transaction, so
        // any increase in its count came from replication.
        SData write("idcollision");
        write["writeConsistency"] = "ASYNC";
        leader.executeWaitVerifyContent(write);

        // The leader committed the command itself, so its count moves first. Checking it separately tells a failure
        // where callbacks never fire at all apart from one where they fire but replication doesn't trigger them.
        uint64_t leaderCountAfter = leaderCountBefore;
        for (int i = 0; i < 100 && leaderCountAfter <= leaderCountBefore; i++) {
            usleep(100'000);
            leaderCountAfter = getAfterCommitCount(leader);
        }
        ASSERT_GREATER_THAN(leaderCountAfter, leaderCountBefore);

        // Replication is asynchronous, so poll rather than assuming the follower has caught up.
        uint64_t followerCountAfter = followerCountBefore;
        for (int i = 0; i < 100 && followerCountAfter <= followerCountBefore; i++) {
            usleep(100'000);
            followerCountAfter = getAfterCommitCount(follower);
        }
        ASSERT_GREATER_THAN(followerCountAfter, followerCountBefore);
    }
} __AfterCommitCallbackClusterTest;
