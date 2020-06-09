#include "../BedrockClusterTester.h"

struct StatusHandlingCommandsTest : tpunit::TestFixture {
    StatusHandlingCommandsTest()
        : tpunit::TestFixture("StatusHandlingCommandsTest",
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
            uint64_t start = STimeNow();
            bool found0, found1, found2;

            while (STimeNow() < start + 6'0000'000 && (!found0 || !found1 || !found2)) {
                result = follower.executeWaitMultipleData({cmd}, 1, false)[0].methodLine;
                if (result == "HTTP/1.1 200 LEADING") {
                    results[0] = result;
                    found0 = true;
                } else if (result == "HTTP/1.1 200 FOLLOWING") {
                    results[1] = result;
                    found1 = true;
                } else if (result == "HTTP/1.1 200 STANDINGDOWN") {
                    results[2] = result;
                    found2 = true;
                }
            }
        });

        leader.stopServer();

        // Execute a slow query while the follower is leading so when the
        // leader is brought back up, it will be STANDINGDOWN until it finishes
        thread slowQueryThread([this, &follower](){
            SData slow("slowquery");
            slow["processTimeout"] = "1000"; // 1s
            follower.executeWaitVerifyContent(slow, "555 Timeout peeking command");
        });

        leader.startServer(true);
        slowQueryThread.join();
        healthCheckThread.join();

        ASSERT_EQUAL(results[0], "HTTP/1.1 200 LEADING")
        ASSERT_EQUAL(results[1], "HTTP/1.1 200 FOLLOWING")
        ASSERT_EQUAL(results[2], "HTTP/1.1 200 STANDINGDOWN")
    }

} __StatusHandlingCommandsTest;
