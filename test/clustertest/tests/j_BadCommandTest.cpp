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

        bool diedCorrectly = false;
        try {
            SData cmd("dieinpeek");
            cmd["userID"] = "31";
            string response = master->executeWaitVerifyContent(cmd);
        } catch (const SException& e) {
            diedCorrectly = (e.what() == "Empty response"s);
        }

        ASSERT_TRUE(diedCorrectly);

        // Wait for something to be mastering.
        sleep(1);

        // Send the same command to a slave. It should blacklist it.
        SData cmd("dieinpeek");
        cmd["userID"] = "31";
        string response = slave->executeWaitVerifyContent(cmd, "500 Blacklisted");

        // Slave blacklisted it ok

        // Try and bring master back up.
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

            // Give it another second...
            sleep(1);
        }

        ASSERT_TRUE(success);

        // Master is back up.

        // Kill it in process.
        diedCorrectly = false;
        try {
            SData cmd("dieinprocess");
            cmd["userID"] = "32";
            string response = master->executeWaitVerifyContent(cmd);
        } catch (const SException& e) {
            diedCorrectly = (e.what() == "Empty response"s);
        }
        ASSERT_TRUE(diedCorrectly);

        // Wait until the old slave was mastering.
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

            // Give it another second...
            sleep(1);
        }

        ASSERT_TRUE(success);

        // Slave promoted to master.

        // Send the same command to the slave (now master).
        cmd = SData("dieinprocess");
        cmd["userID"] = "32";
        response = slave->executeWaitVerifyContent(cmd, "500 Blacklisted");

        // Promoted slave successfully blacklisted command.

        // Kill it in process again with a different userID, since it won't count as blacklisted with a different user.
        diedCorrectly = false;
        try {
            SData cmd("dieinprocess");
            cmd["userID"] = "33";
            string response = slave->executeWaitVerifyContent(cmd);
        } catch (const SException& e) {
            diedCorrectly = (e.what() == "Empty response"s);
        }
        ASSERT_TRUE(diedCorrectly);

        // Bring them both back up.
        tester->startNode(0);
        tester->startNode(1);

        // Wait for master to master.
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

            // Give it another second...
            sleep(1);
        }
        ASSERT_TRUE(success);

        // Wait for slave to slave.
        count = 0;
        success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = slave->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "SLAVING") {
                success = true;
                break;
            }
            // Give it another second...
            sleep(1);
        }
        ASSERT_TRUE(success);

        diedCorrectly = false;
        try {
            SData cmd("generatesegfault");
            cmd["userID"] = "34";
            string response = master->executeWaitVerifyContent(cmd);
        } catch (const SException& e) {
            diedCorrectly = (e.what() == "Empty response"s);
        }
        ASSERT_TRUE(diedCorrectly);

        cmd = SData("generatesegfault");
        cmd["userID"] = "34";
        response = slave->executeWaitVerifyContent(cmd, "500 Blacklisted");
    }

} __j_BadCommandTest;
