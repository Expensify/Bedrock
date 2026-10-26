#include <iostream>

#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct DoubleDetachTest : tpunit::TestFixture
{
    DoubleDetachTest()
        : tpunit::TestFixture("DoubleDetach",
                              BEFORE_CLASS(DoubleDetachTest::setup),
                              AFTER_CLASS(DoubleDetachTest::teardown),
                              TEST(DoubleDetachTest::testDoubleDetach),
                              TEST(DoubleDetachTest::testDetachWaitsForDBPool))
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

    void testDoubleDetach()
    {
        // Test a control command
        BedrockTester& follower = tester->getTester(1);

        SData setPriorityCommand("SetPriority");
        setPriorityCommand["priority"] = "85";
        follower.executeWaitVerifyContent(setPriorityCommand, "200", true);
        ASSERT_TRUE(follower.waitForStatusTerm("priority", "85"));

        // Detach
        SData detachCommand("Detach");
        follower.executeWaitVerifyContent(detachCommand, "203 DETACHING", true);

        // Wait for it to detach
        sleep(3);

        follower.executeWaitVerifyContent(detachCommand, "400 Already detached", true);

        // Re-attach to make shutdown clean.
        SData attachCommand("Attach");
        follower.executeWaitVerifyContent(attachCommand, "204 ATTACHING", true);
        ASSERT_TRUE(follower.waitForStatusTerm("priority", "85"));
    }

    void testDetachWaitsForDBPool()
    {
        BedrockTester& follower = tester->getTester(1);
        const string releaseFile = BedrockTester::getTempFileName();
        unlink(releaseFile.c_str());

        SData holdPool("holddbpool");
        holdPool["releaseFile"] = releaseFile;
        follower.executeWaitVerifyContent(holdPool, "200");
        follower.executeWaitVerifyContent(SData("Detach"), "203 DETACHING", true);

        // Even after the server drops its own references, detach must wait for the borrowed pool and handle.
        const bool syncNodeReleased = follower.waitForStatusTerm("syncNodeAvailable", "false");
        const bool detachedWithPoolHeld = follower.waitForStatusTerm("isDetached", "true", 500'000);
        SFileSave(releaseFile, "release");

        const string releasedFile = releaseFile + ".done";
        const uint64_t deadline = STimeNow() + 5 * STIME_US_PER_S;
        while (!SFileExists(releasedFile) && STimeNow() < deadline) {
            usleep(10'000);
        }
        EXPECT_TRUE(syncNodeReleased);
        EXPECT_FALSE(detachedWithPoolHeld);
        EXPECT_TRUE(SFileExists(releasedFile));
        ASSERT_TRUE(follower.waitForStatusTerm("isDetached", "true"));

        follower.executeWaitVerifyContent(SData("Attach"), "204 ATTACHING", true);
        ASSERT_TRUE(follower.waitForLeadingFollowing());
        unlink(releaseFile.c_str());
        unlink(releasedFile.c_str());
    }
} __DoubleDetachTest;
