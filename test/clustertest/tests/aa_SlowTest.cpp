
#include "../BedrockClusterTester.h"

struct aa_SlowTest : tpunit::TestFixture {
    aa_SlowTest()
        : tpunit::TestFixture("aa_SlowTest",
                              TEST(aa_SlowTest::test)) { }

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
} __aa_SlowTest;

