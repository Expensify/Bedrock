
#include "../BedrockClusterTester.h"

struct i_TimeoutTest : tpunit::TestFixture {
    i_TimeoutTest()
        : tpunit::TestFixture("i_TimeoutTest",
                              BEFORE_CLASS(i_TimeoutTest::setup),
                              AFTER_CLASS(i_TimeoutTest::teardown),
                              TEST(i_TimeoutTest::test),
                              TEST(i_TimeoutTest::testprocess)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        // Test write commands.
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
        BedrockTester* brtester = tester->getBedrockTester(0);

        // Run one long query.
        SData slow("slowprocessquery");
        slow["timeout"] = "500000"; // 0.5s
        slow["size"] = "1000000";
        slow["count"] = "1";
        brtester->executeWaitVerifyContent(slow, "555 Timeout processing command");

        // And a bunch of faster ones.
        slow["size"] = "100";
        slow["count"] = "10000";
        brtester->executeWaitVerifyContent(slow, "555 Timeout processing command");
    }
} __i_TimeoutTest;

