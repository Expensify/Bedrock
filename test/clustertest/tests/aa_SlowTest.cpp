
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
        SData slow("slowquery");
        brtester->executeWaitVerifyContent(slow);
    }
} __aa_SlowTest;

