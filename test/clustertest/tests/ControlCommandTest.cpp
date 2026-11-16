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

        // Remain detached past the ten-second shutdown deadline. Status and Attach must still reply on the control port.
        sleep(9);
        follower.executeWaitVerifyContent(SData("Status"), "200", true);

        // Try to attach again, should be allowed now that the sleep in the plugin
        // has passed.
        follower.executeWaitVerifyContent(attachCommand, "204", true);
    }
} __ControlCommandTest;
