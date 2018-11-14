#include "../BedrockClusterTester.h"

// Waits for a particular port to be free to bind to. This is useful when we've killed a server, because sometimes it
// takes the OS a few seconds to make the port available again.
int waitForPort(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    int i = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &i, sizeof(i));
    sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    int result = 0;
    int count = 0;
    do {
        result = ::bind(sock, (sockaddr*)&addr, sizeof(addr));
        if (result) {
            cout << "Couldn't bind, errno: " << errno << ", '" << strerror(errno) << "'." << endl;
            count++;
            usleep(100'000);
        } else {
            shutdown(sock, 2);
            close(sock);
            return 0;
        }
    // Wait up to 300 10ths of a second (30 seconds).
    } while (result && count++ < 300);

    return 1;
}

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
        string response = master->executeWaitVerifyContent(cmd, "500 Unhandled Exception");

        // Same in process.
        cmd = SData("exceptioninprocess");
        cmd["userID"] = to_string(userID++);
        response = master->executeWaitVerifyContent(cmd, "500 Unhandled Exception");

        // Then for three other commands, verify they kill the master, but the slave then refuses the same command.
        // This tests cases where keeping master alive isn't feasible.
        for (auto cammandName : {"generatesegfaultpeek", "generateassertpeek", "generatesegfaultprocess"}) {
            
            // Create the command with the current userID.
            userID++;
            SData command(cammandName);
            command.methodLine = cammandName;
            command["userID"] = to_string(userID);
            int error = 0;
            master->executeWaitMultipleData({command}, 1, false, true, &error);

            // This error indicates we couldn't read a response after sending a command. We assume this means the
            // server died. Even if it didn't and we just had a weird flaky network connection,  we'll still fail this
            // test if the slave doesn't refuse the same command.
            ASSERT_EQUAL(error, 4);

            // Now send the command to the slave and verify the command was refused.
            slave->executeWaitVerifyContent(command, "500 Refused");

            // TODO: This is where we could send the command with a different userID to the slave and verify it's not
            // refused. We don't currently do this because these commands will kill the slave. We could handle that as
            // the expected case as well, though.

            // Makes ure all it's ports are free and then bring master back up.
            ASSERT_FALSE(waitForPort(tester->getBedrockTester(0)->serverPort()));
            ASSERT_FALSE(waitForPort(tester->getBedrockTester(0)->nodePort()));
            ASSERT_FALSE(waitForPort(tester->getBedrockTester(0)->controlPort()));
            tester->startNode(0);

            // Give it up to a minute to be mastering.
            uint64_t start = STimeNow();
            bool success = false;
            while (STimeNow() < start + 60'000'000) {
                try {
                    STable json = SParseJSONObject(master->executeWaitVerifyContent(SData("Status")));
                    if (json["state"] == "MASTERING") {
                        success = true;
                        break;
                    }
                    // it's not mastering, let it try again.
                } catch (...) {
                    // Doesn't do anything, we'll fall through to the sleep and try again.
                }
                usleep(100'000);
            }
            ASSERT_TRUE(success);
        }
    }

} __BadCommandTest;
