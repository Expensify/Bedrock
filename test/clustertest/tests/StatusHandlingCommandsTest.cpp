#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct StatusHandlingCommandsTest : tpunit::TestFixture {
    StatusHandlingCommandsTest()
        : tpunit::TestFixture("StatusHandlingCommands",
                              BEFORE_CLASS(StatusHandlingCommandsTest::setup),
                              AFTER_CLASS(StatusHandlingCommandsTest::teardown),
                              TEST(StatusHandlingCommandsTest::test)) { }

    BedrockClusterTester* tester;

    void setup () {
        tester = new BedrockClusterTester();
    }

    void teardown() {
        delete tester;
    }

    void test() {
        vector<string> results(3);
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);

        thread healthCheckThread([this, &results, &follower](){
            SData cmd("GET /status/handlingCommands HTTP/1.1");
            string result;
            bool foundLeader = false;
            bool foundFollower = false;
            bool foundStandingdown = false;
            chrono::steady_clock::time_point start = chrono::steady_clock::now();

            while (chrono::steady_clock::now() < start + 60s && (!foundLeader || !foundFollower || !foundStandingdown)) {
                result = follower.executeWaitMultipleData({cmd}, 1, false)[0].methodLine;
                if (result == "HTTP/1.1 200 LEADING") {
                    results[0] = result;
                    foundLeader = true;
                } else if (result == "HTTP/1.1 200 FOLLOWING") {
                    results[1] = result;
                    foundFollower = true;
                } else if (result == "HTTP/1.1 200 STANDINGDOWN") {
                    results[2] = result;
                    foundStandingdown = true;
                }
            }
        });

        leader.stopServer();

        // Execute a slow query while the follower is leading so when the
        // leader is brought back up, it will be STANDINGDOWN until it finishes
        thread slowQueryThread([this, &follower](){
            SData slow("slowquery");
            slow["timeout"] = "5000"; // 5s
            follower.executeWaitVerifyContent(slow, "555 Timeout peeking command");
        });

        leader.startServer(false);
        slowQueryThread.join();
        healthCheckThread.join();

        ASSERT_EQUAL(results[0], "HTTP/1.1 200 LEADING")
        ASSERT_EQUAL(results[1], "HTTP/1.1 200 FOLLOWING")
        ASSERT_EQUAL(results[2], "HTTP/1.1 200 STANDINGDOWN")
    }

} __StatusHandlingCommandsTest;
