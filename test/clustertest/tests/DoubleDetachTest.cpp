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
        cout << "A" << endl;
        BedrockTester& follower = tester->getTester(1);

        // Detach
        cout << "B" << endl;
        SData detachCommand("Detach");
        cout << "C" << endl;
        follower.executeWaitVerifyContent(detachCommand, "203 DETACHING", true);

        // Wait for it to detach
        cout << "D" << endl;
        sleep(3);

        cout << "E" << endl;
        follower.executeWaitVerifyContent(detachCommand, "400 Already detached", true);
        cout << "F" << endl;

        // Re-attach to make shutdown clean.
        SData attachCommand("Attach");
        follower.executeWaitVerifyContent(attachCommand, "204 ATTACHING", true);
        cout << "G" << endl;
    }

} __DoubleDetachTest;
