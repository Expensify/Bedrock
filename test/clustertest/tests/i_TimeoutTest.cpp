
#include "../BedrockClusterTester.h"

struct i_TimeoutTest : tpunit::TestFixture {
    i_TimeoutTest()
        : tpunit::TestFixture("i_TimeoutTest",
                              TEST(i_TimeoutTest::test),
                              TEST(i_TimeoutTest::testprocess)) { }

    BedrockClusterTester* tester;
    void test()
    {
        // Test write commands.
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();
        BedrockTester* brtester = tester->getBedrockTester(0);

        // Run one long query.
        SData slow("slowquery");
        slow["timeout"] = "5000000"; // 5s
        brtester->executeWaitVerifyContent(slow, "555 Timeout peeking command");

        // And a bunch of faster ones.
        slow["size"] = "10000";
        slow["count"] = "10000";
        brtester->executeWaitVerifyContent(slow, "555 Timeout peeking command");
    }

    void testprocess()
    {
        // Test write commands.
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();
        BedrockTester* brtester = tester->getBedrockTester(0);

        // Run one long query.
        SData slow("slowprocessquery");
        slow["timeout"] = "5000000"; // 5s
        brtester->executeWaitVerifyContent(slow, "555 Timeout processing command");

        // And a bunch of faster ones.
        slow["size"] = "10000";
        slow["count"] = "10000";
        brtester->executeWaitVerifyContent(slow, "555 Timeout processing command");
    }
} __i_TimeoutTest;

