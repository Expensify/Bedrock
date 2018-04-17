#include "../BedrockClusterTester.h"

struct j_BadCommandTest : tpunit::TestFixture {
    j_BadCommandTest()
        : tpunit::TestFixture("j_BadCommand",
                              TEST(j_BadCommandTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void test()
    {
        tester = BedrockClusterTester::testers.front();
        BedrockTester* master = tester->getBedrockTester(0);
        BedrockTester* slave = tester->getBedrockTester(1);

        // Make sure unhandled exceptions send the right response.
        SData cmd("dieinpeek");
        cmd["userID"] = "31";
        string response = master->executeWaitVerifyContent(cmd, "500 Unhandled Exception");

        cmd = SData("dieinprocess");
        cmd["userID"] = "31";
        response = master->executeWaitVerifyContent(cmd, "500 Unhandled Exception");

        // Segfault in peek.
        bool diedCorrectly = false;
        try {
            SData cmd("generatesegfaultpeek");
            cmd["userID"] = "32";
            string response = master->executeWaitVerifyContent(cmd);
        } catch (const SException& e) {
            diedCorrectly = (e.what() == "Empty response"s);
        }
        ASSERT_TRUE(diedCorrectly);

        // Send the same command to the slave.
        cmd = SData("generatesegfaultpeek");
        cmd["userID"] = "32";
        response = slave->executeWaitVerifyContent(cmd, "500 Refused");

        // Bring master back up.
        tester->startNode(0);
        int count = 0;
        bool success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = master->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }
            sleep(1);
        }
        ASSERT_TRUE(success);

        // ASSERT in peek.
        diedCorrectly = false;
        try {
            SData cmd("generateassertpeek");
            cmd["userID"] = "32";
            string response = master->executeWaitVerifyContent(cmd);
        } catch (const SException& e) {
            diedCorrectly = (e.what() == "Empty response"s);
        }
        ASSERT_TRUE(diedCorrectly);

        // Send the same command to the slave.
        cmd = SData("generateassertpeek");
        cmd["userID"] = "32";
        response = slave->executeWaitVerifyContent(cmd, "500 Refused");

        // Bring master back up.
        tester->startNode(0);
        count = 0;
        success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = master->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }
            sleep(1);
        }
        ASSERT_TRUE(success);

        // Segfault in process.
        diedCorrectly = false;
        try {
            SData cmd("generatesegfaultprocess");
            cmd["userID"] = "33";
            string response = master->executeWaitVerifyContent(cmd);
        } catch (const SException& e) {
            diedCorrectly = (e.what() == "Empty response"s);
        }
        ASSERT_TRUE(diedCorrectly);

        // Verify the slave is now mastering.
        count = 0;
        success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = slave->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }
            sleep(1);
        }
        ASSERT_TRUE(success);

        // Send the slave the same command, it should be blacklisted.
        cmd = SData("generatesegfaultprocess");
        cmd["userID"] = "33";
        response = slave->executeWaitVerifyContent(cmd, "500 Refused");

        // Try and bring master back up, just because the next test will expect it.
        tester->startNode(0);
        count = 0;
        success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = master->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }
            sleep(1);
        }
        ASSERT_TRUE(success);
    }

} __j_BadCommandTest;
