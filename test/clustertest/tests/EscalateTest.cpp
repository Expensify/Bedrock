
#include "../BedrockClusterTester.h"

struct EscalateTest : tpunit::TestFixture {
    EscalateTest()
        : tpunit::TestFixture("EscalateTest", TEST(EscalateTest::test)) { }

    void test()
    {
        BedrockClusterTester tester;

        // We're going to send a command to a follower.
        BedrockTester& brtester = tester.getTester(1);
        SData cmd("testescalate");
        cmd["writeConsistency"] = "ASYNC";
        brtester.executeWaitMultipleData({cmd});

        // Because the way the above escalation is verified is in the destructor for the command, we send another request to
        // verify the server hasn't crashed.
        SData status("Status");
        auto results = brtester.executeWaitMultipleData({status});
        ASSERT_EQUAL(results.size(), 1);
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
    }
} __EscalateTest;

