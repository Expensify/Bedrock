#include "../BedrockClusterTester.h"

struct BadCommandTest : tpunit::TestFixture {
    BadCommandTest()
        : tpunit::TestFixture("BadCommand",
                              BEFORE_CLASS(BadCommandTest::setup),
                              AFTER_CLASS(BadCommandTest::teardown),
                              TEST(BadCommandTest::test)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID, "");
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        BedrockTester* master = tester->getBedrockTester(0);
        BedrockTester* slave = tester->getBedrockTester(1);

        // This is here because we can use it to test crashIdentifyingValues, though that isn't currently implemented.
        int userID = 31;

        // Make sure unhandled exceptions send an error response, but don't crash the server.
        SData cmd("exceptioninpeek");
        cmd["userID"] = to_string(userID++);
        try {
            master->executeWaitVerifyContent(cmd, "500 Unhandled Exception");
        } catch (...) {
            cout << "failing in first block." << endl;
            throw;
        }

        // Same in process.
        cmd = SData("exceptioninprocess");
        cmd["userID"] = to_string(userID++);
        try {
            master->executeWaitVerifyContent(cmd, "500 Unhandled Exception");
        } catch (...) {
            cout << "failing in second block." << endl;
            throw;
        }

        // Then for three other commands, verify they kill the master, but the slave then refuses the same command.
        // This tests cases where keeping master alive isn't feasible.
        for (auto commandName : {"generatesegfaultpeek", "generateassertpeek", "generatesegfaultprocess"}) {
            
            // Create the command with the current userID.
            userID++;
            SData command(commandName);
            command.methodLine = commandName;
            command["userID"] = to_string(userID);
            int error = 0;
            master->executeWaitMultipleData({command}, 1, false, true, &error);

            // This error indicates we couldn't read a response after sending a command. We assume this means the
            // server died. Even if it didn't and we just had a weird flaky network connection,  we'll still fail this
            // test if the slave doesn't refuse the same command.
            ASSERT_EQUAL(error, 4);

            // Now send the command to the slave and verify the command was refused.
            error = 0;
            vector<SData> results = slave->executeWaitMultipleData({command}, 1, false, false, &error);
            if (results[0].methodLine != "500 Refused") {
                cout << "Didn't get '500 refused', got '" << results[0].methodLine << "' testing '" << commandName << "', error code was set to: " << error << endl;
                ASSERT_TRUE(false);
            }

            // TODO: This is where we could send the command with a different userID to the slave and verify it's not
            // refused. We don't currently do this because these commands will kill the slave. We could handle that as
            // the expected case as well, though.

            // Bring master back up.
            master->startServer();
            ASSERT_TRUE(master->waitForState("MASTERING"));
        }
    }

} __BadCommandTest;
