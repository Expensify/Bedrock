#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct UniqueConstraintsTest : tpunit::TestFixture
{
    UniqueConstraintsTest()
        : tpunit::TestFixture("UniqueConstraints", TEST(UniqueConstraintsTest::test))
    {
    }

    void test()
    {
        BedrockClusterTester tester;
        SData command("Query");
        command["Query"] = "INSERT INTO test VALUES(" + SQ(1) + ", " + SQ("val") + ");";
        auto result1 = tester.getTester(0).executeWaitMultipleData({command})[0];
        auto result2 = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_TRUE(SStartsWith(result1.methodLine, "200"));
        ASSERT_TRUE(SStartsWith(result2.methodLine, "400"));
    }
} __UniqueConstraintsTest;
