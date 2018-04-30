
#include "../BedrockClusterTester.h"

struct TimeoutTest : tpunit::TestFixture {
    TimeoutTest()
        : tpunit::TestFixture("TimeoutTest",
                              BEFORE_CLASS(TimeoutTest::setup),
                              AFTER_CLASS(TimeoutTest::teardown),
                              TEST(TimeoutTest::test),
                              TEST(TimeoutTest::testprocess)) { }

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
        slow["timeout"] = "5000"; // 5s
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
        slow["timeout"] = "500"; // 0.5s
        slow["size"] = "1000000";
        slow["count"] = "1";
        brtester->executeWaitVerifyContent(slow, "555 Timeout processing command");

        // And a bunch of faster ones.
        slow["size"] = "100";
        slow["count"] = "10000";
        brtester->executeWaitVerifyContent(slow, "555 Timeout processing command");
    }
} __TimeoutTest;

