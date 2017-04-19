#include "../BedrockClusterTester.h"

struct g_upgradeDBTest : tpunit::TestFixture {
    g_upgradeDBTest()
        : tpunit::TestFixture("g_upgradeDBTest",
                              TEST(g_upgradeDBTest::test)) { }

    BedrockClusterTester* tester;
    void test()
    {
        return;
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();
        for (auto i : {0,1,2}) {
            BedrockTester* brtester = tester->getBedrockTester(i);

            // This just verifies that the dbupgrade table was created by TestPlugin.
            SData query("Query");
            query["Query"] = "INSERT INTO dbupgrade VALUES(" + SQ(1 + i) + ", " + SQ("val") + ");";
            string result = brtester->executeWait(query, "200");
        }
    }
} __g_upgradeDBTest;

