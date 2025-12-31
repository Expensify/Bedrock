#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct StatusHandlingCommandsTest : tpunit::TestFixture
{
    StatusHandlingCommandsTest()
        : tpunit::TestFixture("StatusHandlingCommands", TEST(StatusHandlingCommandsTest::test))
    {
    }

    void test()
    {
        BedrockClusterTester tester;
        BedrockTester& leader = tester.getTester(0);
        BedrockTester& follower = tester.getTester(1);
        vector<string> results(2);

        leader.stopServer();

        thread healthCheckThread([&results, &follower](){
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

                                         // If we get here, it's not going back to leading/standingdown.
                                         break;
                                     }
                                 }
            });

        sleep(1);
        leader.startServer(false);
        healthCheckThread.join();

        ASSERT_EQUAL(results[0], "HTTP/1.1 200 LEADING")
        ASSERT_EQUAL(results[1], "HTTP/1.1 200 FOLLOWING")
        // We don't test STANDINGDOWN because it's unreliable to get it to show up in the status, we can move straight through it too quickly.
    }
} __StatusHandlingCommandsTest;
