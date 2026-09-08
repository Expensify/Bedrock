/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    StatusHandlingCommandsTest.cpp
 * Path:    test/clustertest/tests/StatusHandlingCommandsTest.cpp
 *
 * INTENT
 *   Cluster test verifying that `GET /status/handlingCommands` on a follower
 *   whose leader is temporarily down reports the follower's own LEADING/
 *   FOLLOWING transition correctly, and never claims a version mismatch just
 *   because it briefly has no leader to compare its version against.
 *
 * OBJECTS
 *   StatusHandlingCommandsTest        - tpunit fixture, single free-standing test.
 *   StatusHandlingCommandsTest::test - stops the leader, polls the follower's status/handlingCommands
 *                                       endpoint on a background thread, then restarts the leader.
 *   __StatusHandlingCommandsTest      - static instance that registers the fixture with tpunit.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with sibling test files.
 * ─────────────────────────────────────────────────────────────────────*/
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

        // While the leader is down the follower loses its lead peer, so its known leader version is empty. All nodes in
        // this cluster share the same version, so a "Mismatched version" response is always wrong here: an empty leader
        // version must be reported as the node's actual state, not as a mismatch.
        atomic<bool> foundMismatch(false);

        leader.stopServer();

        thread healthCheckThread([&results, &follower, &foundMismatch](){
            SData cmd("GET /status/handlingCommands HTTP/1.1");
            string result;
            bool foundLeader = false;
            bool foundFollower = false;
            bool foundStandingdown = false;
            chrono::steady_clock::time_point start = chrono::steady_clock::now();

            while (chrono::steady_clock::now() < start + 60s && (!foundLeader || !foundFollower || !foundStandingdown)) {
                result = follower.executeWaitMultipleData({cmd}, 1, false)[0].methodLine;
                if (SContains(result, "Mismatched version")) {
                    foundMismatch = true;
                }
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
        ASSERT_FALSE(foundMismatch)
        // We don't test STANDINGDOWN because it's unreliable to get it to show up in the status, we can move straight through it too quickly.
    }
} __StatusHandlingCommandsTest;
