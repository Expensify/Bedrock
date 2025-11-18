#include <iostream>

#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

/* This class currently tests two important cases for fast stand down, 1 that we can stand down with commands currently running (the HTTPS test does this)
 * And 2, that we can stand down with commands queued (the future commands test does this).
 * It does not exhaustively test every case because they're almost impossible to test, largely for timing reasons.
 *
 * Some cases that would be nice to test:
 * * Commands in the blocking commit queue.
 * * Commands in the middle of running `process()`.
 * * Commands where the node state change in the call to `commit()`.
 *
 * Unfortunately these are very hard to do, and the expectation is that these cases work very similarly to the cases we DO have tests for.
 */
struct FastStandDownTest : tpunit::TestFixture
{
    FastStandDownTest()
        : tpunit::TestFixture("FastStandDown",
                              BEFORE_CLASS(FastStandDownTest::setup),
                              AFTER_CLASS(FastStandDownTest::teardown),
                              TEST(FastStandDownTest::testHTTPSRequests),
                              TEST(FastStandDownTest::testFutureCommands))
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    void testHTTPSRequests()
    {
        // Send a command that makes a slow HTTPS request.
        SData httpsRequest("httpstimeout");
        httpsRequest["waitFor"] = "5"; // Wait for 5 seconds before sending.
        vector<SData> httpsResult;
        thread t1([&]() {
                  httpsResult = tester->getTester(0).executeWaitMultipleData({httpsRequest}, 1);
            });
        uint64_t commandStartTime = STimeNow();

        // Stop leader, but don't wait for it to finish yet.
        thread t2([&]() {
                  // Allow for our command to have sent.
                  sleep(1);
                  tester->stopNode(0);
            });

        // Wait for secondary to be leading. We can't wait for the old leader to be following, because it will have closed its ports so we can't check its state.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));

        // Check that the follower became leader while the command on the old leader should still have been stuck in its 5s slow HTTPS send.
        ASSERT_LESS_THAN((STimeNow() - commandStartTime), 4'000'000);

        // Wait for the response to the command.
        t1.join();

        // And for our leader to finish shutting down.
        t2.join();

        // verify the response was processed on `cluster_node_1`. It was sent to `cluster_node_0`, so this means it was escalated.
        // For it to be escalated, node_0 would have needed to be FOLLOWING, which implies it ran through the correct: LEADING->FOLLOWING->OFF series of events.
        ASSERT_EQUAL(httpsResult[0]["processingNode"], "cluster_node_1");
    }

    void testFutureCommands()
    {
        // Because we'd stopped this in the previous test, we need to start it again.
        tester->startNode(0);

        // Create a command that will run 5 seconds from now.
        SData insertQuery("Query");
        insertQuery["commandExecuteTime"] = to_string(STimeNow() + 5'000'000);
        insertQuery["Query"] = "INSERT INTO test VALUES(" + insertQuery["commandExecuteTime"] + ", " + SQ("escalated_in_the_past") + ");";
        vector<SData> httpsResult;
        thread t1([&]() {
                  httpsResult = tester->getTester(0).executeWaitMultipleData({insertQuery}, 1);
            });
        uint64_t commandStartTime = STimeNow();

        // Stop leader, but don't wait for it to finish yet.
        uint64_t nodeStopStime = 0;
        thread t2([&]() {
                  // Allow for all of our commands to have sent.
                  sleep(1);
                  tester->stopNode(0);
                  nodeStopStime = STimeNow();
            });

        // Wait for secondary to be leading. We can't wait for the old leader to be following, because it will have closed its ports and so we can't check its state.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));

        // Check that the follower became leader while the command on the old leader should still have been stuck in its 5s wait.
        ASSERT_LESS_THAN((STimeNow() - commandStartTime), 4'000'000);

        // Wait for responses for our commands.
        t1.join();

        // And for our leader to finish shutting down.
        t2.join();

        // Verify that leader couldn't shut down until after the 5s delay we introduced.
        ASSERT_GREATER_THAN(nodeStopStime, stoull(insertQuery["commandExecuteTime"]));

        // Verify the DB on the new leader contains the commit.
        SData lookupQuery("Query");
        lookupQuery["Query"] = "SELECT COUNT(*) FROM test WHERE id = " + insertQuery["commandExecuteTime"] + " AND value = 'escalated_in_the_past';";
        httpsResult = tester->getTester(1).executeWaitMultipleData({lookupQuery}, 1);
        ASSERT_EQUAL(httpsResult[0].content, "COUNT(*)\n1\n");
    }
} __FastStandDownTest;
