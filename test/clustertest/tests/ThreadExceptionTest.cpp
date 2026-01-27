#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ThreadExceptionTest : tpunit::TestFixture
{
    ThreadExceptionTest()
        : tpunit::TestFixture("ThreadException", TEST(ThreadExceptionTest::test))
    {
    }

    void test()
    {
        BedrockClusterTester tester;

        SData command("ThreadException");
        SData result = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_EQUAL(result.methodLine, "200 OK");

        command["rethrow"] = "true";
        result = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_EQUAL(result.methodLine, "500 THREAD THREW");
    }
} __ThreadExceptionTest;
