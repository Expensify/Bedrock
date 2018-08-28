
#include "../BedrockClusterTester.h"

struct ControlCommandTest : tpunit::TestFixture {
    ControlCommandTest()
        : tpunit::TestFixture("ControlCommand",
                              BEFORE_CLASS(ControlCommandTest::setup),
                              AFTER_CLASS(ControlCommandTest::teardown),
                              TEST(ControlCommandTest::testPreventAttach),
                              TEST(ControlCommandTest::testSuppressCommandPort)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown() {
        delete tester;
    }

    void testPreventAttach()
    {
        // Test a control command
        BedrockTester* slave = tester->getBedrockTester(1);

        // Tell the plugin to prevent attaching
        SData command("preventattach");
        slave->executeWaitVerifyContent(command, "200");

        // Detach
        SData detachCommand("detach");
        slave->executeWaitVerifyContent(detachCommand, "203", true);

        // Wait for it to detach
        sleep(3);

        // Try to attach
        SData attachCommand("attach");
        slave->executeWaitVerifyContent(attachCommand, "401 Attaching prevented by TestPlugin", true);

        sleep(5);

        // Try to attach again, should be allowed now that the sleep in the plugin
        // has passed.
        slave->executeWaitVerifyContent(attachCommand, "204", true);
    }

    void testSuppressCommandPort()
    {
        // Pick a slave to test on
        BedrockTester* slave = tester->getBedrockTester(1);

        // The three commands we'll need for this test
        SData suppress("SuppressCommandPort");
        SData clear("ClearCommandPort");
        SData status("Status");

        // Basic case, open then close it.
        slave->executeWaitVerifyContent(suppress, "200", true);
        slave->executeWaitVerifyContent(clear, "200", true);

        // Failure case 1, more suppressions than clears
        slave->executeWaitVerifyContent(suppress, "200", true);
        slave->executeWaitVerifyContent(suppress, "200", true);
        slave->executeWaitVerifyContent(clear, "201", true);

        // Ensure the port is actually closed.
        slave->executeWaitVerifyContent(status, "002");

        // Send one more clear to get the counter back to 0
        slave->executeWaitVerifyContent(clear, "200", true);

        // Ensure the port was reopened
        int count = 0;
        bool success = false;
        while (count++ < 50) {
            try {
                string response = slave->executeWaitVerifyContent(status);
                STable json = SParseJSONObject(response);
                if (json["state"] == "SLAVING") {
                    success = true;
                    break;
                }
            } catch (const SException& e) {
                // just try again
            }

            // Give it another second...
            sleep(1);
        }

        ASSERT_TRUE(success);

        // Failure case 2, more clears than suppressions
        slave->executeWaitVerifyContent(suppress, "200", true);
        slave->executeWaitVerifyContent(clear, "200", true);
        slave->executeWaitVerifyContent(clear, "200", true);

        // Ensure the port is actually opened, and counter didn't go below 0
        count = 0;
        success = false;
        while (count++ < 50) {
            try {
                string response = slave->executeWaitVerifyContent(status);
                STable json = SParseJSONObject(response);
                if (json["state"] == "SLAVING") {
                    ASSERT_EQUAL(json["CommandPortClosures"], "0");
                    success = true;
                    break;
                }
            } catch (const SException& e) {
                // just try again
            }

            // Give it another second...
            sleep(1);
        }

        ASSERT_TRUE(success);
    }

} __ControlCommandTest;
