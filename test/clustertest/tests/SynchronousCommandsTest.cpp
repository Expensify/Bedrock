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
                              TEST(SynchronousCommandsTest::commitLockTimeoutFailsCommand),
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

    void commitLockTimeoutFailsCommand()
    {
        // The blocking commit thread holds the commit lock for its whole transaction, so a synchronous command that
        // sleeps in `process` keeps every other write waiting for the lock.
        SData holder("reportcommitthread");
        holder["value"] = "holder";
        holder["ProcessSleep"] = "3000";
        vector<SData> holderResults;
        thread holderThread([&]() {
            holderResults = tester->getTester(0).executeWaitMultipleData({holder});
        });

        // Give the holder time to take the lock, then send a write that can only wait 1 s for it.
        usleep(500'000);
        SData waiter("reportcommitthreadunlisted");
        waiter["value"] = "waiter";
        waiter["processTimeout"] = "1000";
        vector<SData> waiterResults = tester->getTester(0).executeWaitMultipleData({waiter});
        holderThread.join();

        // The waiter fails instead of retrying until the holder lets go, and the holder still commits.
        ASSERT_EQUAL(waiterResults[0].methodLine, "555 Timeout waiting for commit lock");
        ASSERT_EQUAL(holderResults[0].methodLine, "200 OK");
        ASSERT_EQUAL(tester->getTester(0).readDB("SELECT COUNT(*) FROM test WHERE value = 'waiter';"), "0");
    }
} __SynchronousCommandsTest;
