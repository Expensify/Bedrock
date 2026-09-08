/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    ControlCommandTest.cpp
 * Path:    test/clustertest/tests/ControlCommandTest.cpp
 *
 * INTENT
 *   Cluster test verifying that a "preventattach" control command sent to
 *   the test plugin blocks a subsequent Attach from succeeding until its
 *   internal delay has passed.
 *
 * OBJECTS
 *   ControlCommandTest                    - tpunit fixture; brings up a default cluster.
 *   ControlCommandTest::testPreventAttach - detaches a follower, tells the plugin to prevent
 *                                            attaching, confirms Attach is refused then later allowed.
 *   __ControlCommandTest                  - static instance that registers the fixture with tpunit.
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

struct ControlCommandTest : tpunit::TestFixture
{
    ControlCommandTest()
        : tpunit::TestFixture("ControlCommand",
                              BEFORE_CLASS(ControlCommandTest::setup),
                              AFTER_CLASS(ControlCommandTest::teardown),
                              TEST(ControlCommandTest::testPreventAttach))
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

    void testPreventAttach()
    {
        // Test a control command
        BedrockTester& follower = tester->getTester(1);

        // Tell the plugin to prevent attaching
        SData command("preventattach");
        follower.executeWaitVerifyContent(command, "200");

        // Detach
        SData detachCommand("detach");
        follower.executeWaitVerifyContent(detachCommand, "203", true);

        // Wait for it to detach
        sleep(3);
        // Try to attach
        SData attachCommand("attach");
        follower.executeWaitVerifyContent(attachCommand, "401 Attaching prevented by TestPlugin", true);

        sleep(5);

        // Try to attach again, should be allowed now that the sleep in the plugin
        // has passed.
        follower.executeWaitVerifyContent(attachCommand, "204", true);
    }
} __ControlCommandTest;
