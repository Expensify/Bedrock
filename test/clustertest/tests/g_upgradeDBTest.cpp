#include "../BedrockClusterTester.h"

struct g_upgradeDBTest : tpunit::TestFixture {
    g_upgradeDBTest()
        : tpunit::TestFixture("g_upgradeDBTest",
                              BEFORE_CLASS(g_upgradeDBTest::setup),
                              AFTER_CLASS(g_upgradeDBTest::teardown),
                              TEST(g_upgradeDBTest::test)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        for (auto i : {0,1,2}) {
            BedrockTester* brtester = tester->getBedrockTester(i);

            // This just verifies that the dbupgrade table was created by TestPlugin.
            SData query("Query");
            query["Query"] = "INSERT INTO dbupgrade VALUES(" + SQ(1 + i) + ", " + SQ("val") + ");";
            string result = brtester->executeWaitVerifyContent(query, "200");
        }
    }
} __g_upgradeDBTest;

