#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct BadCommandTest : tpunit::TestFixture {
    BadCommandTest()
        : tpunit::TestFixture("BadCommand", TEST(BadCommandTest::test)) { }

    void test()
    {
        bool success = false;

        // Since this is not guaranteed to succeed, try up to 3 times, and if any of them succeed, count as a success.
        for (int i = 0; i < 2; i++) {
            try {
                BedrockClusterTester tester;
                BedrockTester& leader = tester.getTester(0);
                BedrockTester& follower = tester.getTester(1);

                ASSERT_TRUE(tester.getTester(0).waitForState("LEADING"));
                ASSERT_TRUE(tester.getTester(1).waitForState("FOLLOWING"));
                ASSERT_TRUE(tester.getTester(2).waitForState("FOLLOWING"));

                int userID = 31;

                // Make sure unhandled exceptions send an error response, but don't crash the server.
                SData cmd("exceptioninpeek");
                cmd["userID"] = to_string(userID++);
                try {
                    leader.executeWaitVerifyContent(cmd, "500 Unhandled Exception");
                } catch (...) {
                    cout << "[BadCommandTest] failing in first block." << endl;
                    throw;
                }

                // Same in process.
                cmd = SData("exceptioninprocess");
                cmd["userID"] = to_string(userID++);
                try {
                    leader.executeWaitVerifyContent(cmd, "500 Unhandled Exception");
                } catch (...) {
                    cout << "[BadCommandTest] failing in second block." << endl;
                    throw;
                }

                // Then for three other commands, verify they kill the leader, but the follower then refuses the same command.
                // This tests cases where keeping leader alive isn't feasible.
                bool testFailed = false;
                for (auto commandName : {"generatesegfaultpeek", "generateassertpeek", "generatesegfaultprocess"}) {

                    // Create the command with the current userID.
                    userID++;
                    SData command(commandName);
                    command.methodLine = commandName;
                    command["userID"] = to_string(userID);
                    int error = 0;
                    leader.executeWaitMultipleData({command}, 1, false, true, &error);

                    // Wait for the follower to become leader.
                    bool leading = false;
                    for (int i = 0; i < 500; i++) {
                        SData status("Status");
                        vector<SData> statusResult = follower.executeWaitMultipleData({status}, 1, true);
                        STable json = SParseJSONObject(statusResult[0].content);
                        if (json["state"] == "LEADING") {
                            leading = true;
                            break;
                        }
                        usleep(100'00);
                    }
                    if (!leading) {
                        testFailed = true;
                        break;
                    }

                    // This error indicates we couldn't read a response after sending a command. We assume this means the
                    // server died. Even if it didn't and we just had a weird flaky network connection,  we'll still fail this
                    // test if the follower doesn't refuse the same command.
                    if (error != 4) {
                        testFailed = true;
                        break;
                    }

                    // Now send the command to the follower and verify the command was refused.
                    error = 0;
                    vector<SData> results = follower.executeWaitMultipleData({command}, 1, false, false, &error);
                    if (results[0].methodLine != "500 Refused") {
                        cout << "[BadCommandTest] Didn't get '500 refused', got '" << results[0].methodLine << "' testing '" << commandName << "', error code was set to: " << error << endl;
                        testFailed = true;
                        break;
                    }

                    // TODO: This is where we could send the command with a different userID to the follower and verify it's not
                    // refused. We don't currently do this because these commands will kill the follower. We could handle that as
                    // the expected case as well, though.

                    // Bring leader back up.
                    leader.startServer();
                    if (!leader.waitForState("LEADING", 10'000'000)) {
                        testFailed = true;
                        break;
                    }
                }

                if (!testFailed) {
                    success = true;
                    break;
                }
            } catch (...) {
                cout << "Caught exception running test." << endl;
            }
        }
        ASSERT_TRUE(success);
    }

} __BadCommandTest;
