#include "../BedrockClusterTester.h"

struct BadCommandTest : tpunit::TestFixture {
    BadCommandTest()
        : tpunit::TestFixture("BadCommand",
                              BEFORE_CLASS(BadCommandTest::setup),
                              AFTER_CLASS(BadCommandTest::teardown),
                              TEST(BadCommandTest::test)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester("" /*Explicitly exclude the default plugins. TODO: Why does this help?*/);
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);

        int userID = 31;

        // Make sure unhandled exceptions send an error response, but don't crash the server.
        SData cmd("exceptioninpeek");
        cmd["userID"] = to_string(userID++);
        try {
            leader.executeWaitVerifyContent(cmd, "500 Unhandled Exception");
        } catch (...) {
            cout << "failing in first block." << endl;
            throw;
        }

        // Same in process.
        cmd = SData("exceptioninprocess");
        cmd["userID"] = to_string(userID++);
        try {
            leader.executeWaitVerifyContent(cmd, "500 Unhandled Exception");
        } catch (...) {
            cout << "failing in second block." << endl;
            throw;
        }

        // Then for three other commands, verify they kill the leader, but the follower then refuses the same command.
        // This tests cases where keeping leader alive isn't feasible.
        for (auto commandName : {"generatesegfaultpeek", "generateassertpeek", "generatesegfaultprocess"}) {
            
            // Create the command with the current userID.
            userID++;
            SData command(commandName);
            command.methodLine = commandName;
            command["userID"] = to_string(userID);
            int error = 0;
            leader.executeWaitMultipleData({command}, 1, false, true, &error);

            // This error indicates we couldn't read a response after sending a command. We assume this means the
            // server died. Even if it didn't and we just had a weird flaky network connection,  we'll still fail this
            // test if the follower doesn't refuse the same command.
            ASSERT_EQUAL(error, 4);

            // Now send the command to the follower and verify the command was refused.
            error = 0;
            vector<SData> results = follower.executeWaitMultipleData({command}, 1, false, false, &error);
            if (results[0].methodLine != "500 Refused") {
                cout << "Didn't get '500 refused', got '" << results[0].methodLine << "' testing '" << commandName << "', error code was set to: " << error << endl;
                ASSERT_TRUE(false);
            }

            // TODO: This is where we could send the command with a different userID to the follower and verify it's not
            // refused. We don't currently do this because these commands will kill the follower. We could handle that as
            // the expected case as well, though.

            // Bring leader back up.
            leader.startServer();
            ASSERT_TRUE(leader.waitForStates({"LEADING", "MASTERING"}));
        }
    }

} __BadCommandTest;
