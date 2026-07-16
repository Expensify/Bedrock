#include "libstuff/libstuff.h"
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct EscalateTest : tpunit::TestFixture
{
    EscalateTest() : tpunit::TestFixture("Escalate", BEFORE_CLASS(EscalateTest::setup),
                                         AFTER_CLASS(EscalateTest::teardown),
                                         TEST(EscalateTest::test),
                                         TEST(EscalateTest::testSerializedData),
                                         TEST(EscalateTest::testSerializedDataException),
                                         TEST(EscalateTest::socketReuse),
                                         TEST(EscalateTest::duplicateEscalatedRequestDetected))
    {
    }

    BedrockClusterTester* tester = nullptr;

    void setup()
    {
        tester = new BedrockClusterTester;
    }

    void teardown()
    {
        delete tester;
    }

    // NOTE: This test relies on two processes (the leader and follower Bedrock nodes) both writing to the same temp
    // file at potentially the same time. It's not impossible that these two writes step on each other and this test
    // fails because the file ends up in a corrupt state. If we see intermittent failures in this test, we may want to
    // add some sort of file locking to the reading/writing from this file.
    void test()
    {
        // We're going to send a command to a follower.
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("testescalate");
        cmd["writeConsistency"] = "ASYNC";
        cmd["tempFile"] = BedrockTester::getTempFileName("escalate_test");
        brtester.executeWaitMultipleData({cmd});

        // Sleep for 1 second to make sure, if there was a crash, that the next command does not run before the server
        // fully crashes.
        sleep(1);

        // Because the way the above escalation is verified is in the destructor for the command, we send another request to
        // verify the server hasn't crashed.
        SData status("Status");
        auto results = brtester.executeWaitMultipleData({status});
        ASSERT_EQUAL(results.size(), 1);
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        SFileDelete(cmd["tempFile"]);
    }

    void testSerializedData()
    {
        // We're going to escalate from follower 1.
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("EscalateSerializedData");
        SData result = brtester.executeWaitMultipleData({cmd})[0];

        // Parse the results, which should contain the node name we escalated from,
        // the node name we escalated to, and the test name.
        auto resultComponenets = SParseList(result.content, ':');

        // Validate the results.
        ASSERT_EQUAL("cluster_node_0", resultComponenets.front());
        resultComponenets.pop_front();
        ASSERT_EQUAL("cluster_node_1", resultComponenets.front());
        resultComponenets.pop_front();
        ASSERT_EQUAL("Escalate", resultComponenets.front());
        resultComponenets.pop_front();
    }

    void testSerializedDataException()
    {
        // We're going to escalate from follower 1.
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("EscalateSerializedData");
        cmd["throw"] = "true";
        SData result = brtester.executeWaitMultipleData({cmd})[0];
        ASSERT_EQUAL("500 BAD DESERIALIZE", result.methodLine);
    }

    // Note: see this PR: https://github.com/Expensify/Bedrock/pull/1308 for the reasoning behind this test.
    void socketReuse()
    {
        // We're going to escalate from follower 1.
        BedrockTester& brtester = tester->getTester(1);

        // Build a command.
        SData cmd("testescalate");
        cmd["writeConsistency"] = "ASYNC";
        cmd["tempFile"] = BedrockTester::getTempFileName("escalate_test");

        // Set this so the follower's socket to leader won't get reused.
        cmd["Connection"] = "close";

        // Ok, send the command.
        auto results = brtester.executeWaitMultipleData({cmd});
        ASSERT_EQUAL(results[0].methodLine, "200 OK");

        // Let the follower put the escalation socket back in the pool for reuse, or not (the intended case).
        usleep(100'000);

        // Send the command again. It shouldn't fail with a broken socket.
        results = brtester.executeWaitMultipleData({cmd});
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
    }

    // A follower that escalates the same request to the leader more than once while a copy is still being processed
    // causes write amplification. The leader detects these duplicates (matching on the command id) and reports a
    // running count via `Status`. Here we escalate two copies of one request sharing the same id, overlapping in time
    // via a slow process step, and verify the leader noticed the duplicate.
    void duplicateEscalatedRequestDetected()
    {
        // Node 0 is the leader (which tracks duplicates), node 1 a follower (which we escalate from).
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));

        // Read the leader's current duplicate count so the assertion is independent of any earlier tests.
        uint64_t countBefore = SToUInt64(SParseJSONObject(leader.executeWaitVerifyContent(SData("Status"), "200", true))["duplicateEscalatedRequestCount"]);

        // Two identical write commands sharing one id. The leader reuses the escalated request's id, so both copies
        // land under the same id. The slow process step keeps the first copy in flight long enough for the second to
        // arrive while the first is still being processed on the leader.
        SData cmd("idcollision");
        cmd["writeConsistency"] = "ASYNC";
        cmd["ID"] = "duplicate_escalation_test";
        cmd["ProcessSleep"] = "1000";
        cmd["value"] = "duplicate";

        // Send both at once (one per connection) so they escalate to the leader concurrently.
        auto results = follower.executeWaitMultipleData({cmd, cmd}, 2);
        ASSERT_EQUAL(results.size(), 2);
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        ASSERT_EQUAL(results[1].methodLine, "200 OK");

        // The leader should have detected at least one duplicate.
        uint64_t countAfter = SToUInt64(SParseJSONObject(leader.executeWaitVerifyContent(SData("Status"), "200", true))["duplicateEscalatedRequestCount"]);
        ASSERT_GREATER_THAN(countAfter, countBefore);
    }
} __EscalateTest;
