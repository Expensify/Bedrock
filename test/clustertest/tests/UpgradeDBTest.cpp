#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct UpgradeDBTest : tpunit::TestFixture {
    UpgradeDBTest()
        : tpunit::TestFixture("UpgradeDB",
                              BEFORE_CLASS(UpgradeDBTest::setup),
                              AFTER_CLASS(UpgradeDBTest::teardown),
                              TEST(UpgradeDBTest::test)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester();
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        for (auto i : {0,1,2}) {
            BedrockTester& brtester = tester->getTester(i);

            // This just verifies that the dbupgrade table was created by TestPlugin.
            SData query("Query");
            query["Query"] = "INSERT INTO dbupgrade VALUES(" + SQ(1 + i) + ", " + SQ("val") + ");";
            brtester.executeWaitVerifyContent(query, "200");
        }
    }
} __UpgradeDBTest;

