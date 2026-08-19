#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/clustertest/BedrockClusterTester.h>

struct WriteLocalUnreplicatedClusterTest : tpunit::TestFixture
{
    WriteLocalUnreplicatedClusterTest()
        : tpunit::TestFixture("WriteLocalUnreplicatedCluster",
                              BEFORE_CLASS(WriteLocalUnreplicatedClusterTest::setup),
                              AFTER_CLASS(WriteLocalUnreplicatedClusterTest::teardown),
                              TEST(WriteLocalUnreplicatedClusterTest::doesNotReachPeers))
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

    int64_t countRowsWithID(BedrockTester& node, int64_t id)
    {
        SQResult result;
        node.readDB("SELECT COUNT(*) FROM test WHERE id = " + SQ(id) + ";", result);
        return SToInt64(result[0][0]);
    }

    void doesNotReachPeers()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));

        // Write two rows the normal way, so they replicate to the follower. The row we delete below has to be the
        // older of the two: idcollision picks its primary key with MAX(id) + 1, so deleting the highest id here would
        // hand the same key back to the next insert. That insert succeeds on the leader, which no longer has the row,
        // and then breaks the follower, which does. Keeping a higher row alive on both nodes avoids that, and is a
        // concrete example of what "no other node depends on this row" has to mean in practice.
        leader.executeWaitVerifyContent(SData("idcollision"));

        SQResult result;
        leader.readDB("SELECT MAX(id) FROM test;", result);
        const int64_t id = SToInt64(result[0][0]);

        leader.executeWaitVerifyContent(SData("idcollision"));

        // Both nodes have it before we start.
        ASSERT_EQUAL(countRowsWithID(leader, id), 1);
        for (int i = 0; i < 100 && countRowsWithID(follower, id) == 0; i++) {
            usleep(100'000);
        }
        ASSERT_EQUAL(countRowsWithID(follower, id), 1);

        // Delete it on the leader through writeLocalUnreplicated. The command hands the work to the plugin's deleter
        // thread, so poll for the result rather than expecting it to have happened by the time the response lands.
        SData deleteCommand("deletetestrowunreplicated");
        deleteCommand["id"] = SToStr(id);
        leader.executeWaitVerifyContent(deleteCommand);

        for (int i = 0; i < 100 && countRowsWithID(leader, id) != 0; i++) {
            usleep(100'000);
        }
        ASSERT_EQUAL(countRowsWithID(leader, id), 0);

        // Still on the follower, because the delete was never journaled and so never shipped. Give replication the
        // same window it got above, so this is a real absence of replication rather than a race we happened to win.
        for (int i = 0; i < 10; i++) {
            usleep(100'000);
        }
        ASSERT_EQUAL(countRowsWithID(follower, id), 1);

        // And the cluster is still healthy: the leader can still commit normally afterwards.
        leader.executeWaitVerifyContent(SData("idcollision"));
        ASSERT_TRUE(leader.waitForState("LEADING"));
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));
    }
} __WriteLocalUnreplicatedClusterTest;
