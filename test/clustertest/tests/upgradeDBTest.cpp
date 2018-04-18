#include "../BedrockClusterTester.h"

struct upgradeDBTest : tpunit::TestFixture {
    upgradeDBTest()
        : tpunit::TestFixture("upgradeDBTest",
                              BEFORE_CLASS(upgradeDBTest::setup),
                              AFTER_CLASS(upgradeDBTest::teardown),
                              TEST(upgradeDBTest::test)) { }

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
} __upgradeDBTest;

