#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

// Verifies that `-synchronousCommands` routes the commands it names to the blocking commit thread (worker 0), both when
// the command arrives at the leader directly and when it arrives at a follower and is escalated.
//
// The `reportcommitthread` test command is a plain write that reports which thread committed it, so we can tell where
// it ran. The escalation case matters because the flag doesn't travel with the command: the leader re-derives it from
// the command name when it builds the escalated request.
struct SynchronousCommandsTest : tpunit::TestFixture
{
    SynchronousCommandsTest()
        : tpunit::TestFixture("SynchronousCommands",
                              BEFORE_CLASS(SynchronousCommandsTest::setup),
                              TEST(SynchronousCommandsTest::onLeader),
                              TEST(SynchronousCommandsTest::escalatedFromFollower),
                              TEST(SynchronousCommandsTest::unlistedCommandIsUnaffected),
                              AFTER_CLASS(SynchronousCommandsTest::teardown))
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester(ClusterSize::THREE_NODE_CLUSTER, {}, {{"-synchronousCommands", "reportcommitthread"}});
    }

    void teardown()
    {
        delete tester;
    }

    void onLeader()
    {
        SData request("reportcommitthread");
        request["value"] = "leader";
        vector<SData> results = tester->getTester(0).executeWaitMultipleData({request});
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        ASSERT_EQUAL(results[0]["blockingCommitThread"], "true");
    }

    void escalatedFromFollower()
    {
        SData request("reportcommitthread");
        request["value"] = "follower";
        vector<SData> results = tester->getTester(1).executeWaitMultipleData({request});
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        ASSERT_EQUAL(results[0]["blockingCommitThread"], "true");
    }

    void unlistedCommandIsUnaffected()
    {
        // The negative control, and the reason the assertions above mean anything: without it, a bug that sent every
        // command to the blocking thread would still pass this fixture.
        //
        // `-synchronousCommands` matches the method line exactly, while the test plugin dispatches on a prefix. So
        // this reaches the same handler as `reportcommitthread` but isn't in the list, and should commit on a regular
        // worker thread.
        SData request("reportcommitthreadunlisted");
        request["value"] = "unlisted";
        vector<SData> results = tester->getTester(0).executeWaitMultipleData({request});
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        ASSERT_EQUAL(results[0]["blockingCommitThread"], "false");
    }
} __SynchronousCommandsTest;
