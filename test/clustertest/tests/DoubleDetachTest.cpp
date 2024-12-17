#include <iostream>

#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct DoubleDetachTest : tpunit::TestFixture {
    DoubleDetachTest()
        : tpunit::TestFixture("DoubleDetach",
                              BEFORE_CLASS(DoubleDetachTest::setup),
                              AFTER_CLASS(DoubleDetachTest::teardown),
                              TEST(DoubleDetachTest::testDoubleDetach)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester();
    }

    void teardown() {
        delete tester;
    }

    void testDoubleDetach()
    {
        // Test a control command
        BedrockTester& follower = tester->getTester(1);

        // Detach
        SData detachCommand("Detach");
        follower.executeWaitVerifyContent(detachCommand, "203 DETACHING", true);

        // Wait for it to detach
        sleep(3);

        follower.executeWaitVerifyContent(detachCommand, "400 Already detached", true);

        // Re-attach to make shutdown clean.
        SData attachCommand("Attach");
        follower.executeWaitVerifyContent(attachCommand, "204 ATTACHING", true);
    }

} __DoubleDetachTest;
