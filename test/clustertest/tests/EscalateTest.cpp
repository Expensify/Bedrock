#include "libstuff/libstuff.h"
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct EscalateTest : tpunit::TestFixture {
    EscalateTest() : tpunit::TestFixture("Escalate", BEFORE_CLASS(EscalateTest::setup),
                                                     AFTER_CLASS(EscalateTest::teardown),
                                                     TEST(EscalateTest::test),
                                                     TEST(EscalateTest::testSerializedData),
                                                     TEST(EscalateTest::socketReuse)) { }

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

        // Parse the results, which shoould contain the node name we escalated from,
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
} __EscalateTest;
