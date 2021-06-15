#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct EscalateTest : tpunit::TestFixture {
    EscalateTest() : tpunit::TestFixture("EscalateTest", TEST(EscalateTest::test)) { }

    // NOTE: This test relies on two processes (the leader and follower Bedrock nodes) both writing to the same temp
    // file at potentially the same time. It's not impossible that these two writes step on each other and this test
    // fails because the file ends up in a corrupt state. If we see intermittent failures in this test, we may want to
    // add some sort of file locking to the reading/writing from this file.
    void test()
    {
        BedrockClusterTester tester;

        // We're going to send a command to a follower.
        BedrockTester& brtester = tester.getTester(1);
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
} __EscalateTest;
