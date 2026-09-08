/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    DoubleDetachTest.cpp
 * Path:    test/clustertest/tests/DoubleDetachTest.cpp
 *
 * INTENT
 *   Cluster test verifying that detaching an already-detached follower is
 *   rejected ("Already detached") rather than accepted a second time, and
 *   that the node re-attaches cleanly and keeps its priority afterward.
 *
 * OBJECTS
 *   DoubleDetachTest                    - tpunit fixture; brings up a default cluster.
 *   DoubleDetachTest::testDoubleDetach - sets a follower's priority, detaches it twice, then re-attaches.
 *   __DoubleDetachTest                  - static instance that registers the fixture with tpunit.
 *
 * OUT OF PLACE
 *   [CANDIDATE] #include <iostream> - unused in this file.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with sibling test files.
 * ─────────────────────────────────────────────────────────────────────*/
#include <iostream>

#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct DoubleDetachTest : tpunit::TestFixture
{
    DoubleDetachTest()
        : tpunit::TestFixture("DoubleDetach",
                              BEFORE_CLASS(DoubleDetachTest::setup),
                              AFTER_CLASS(DoubleDetachTest::teardown),
                              TEST(DoubleDetachTest::testDoubleDetach))
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
} __DoubleDetachTest;
