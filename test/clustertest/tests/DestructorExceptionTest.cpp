#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct DestructorExceptionTest : tpunit::TestFixture
{
    DestructorExceptionTest()
        : tpunit::TestFixture("DestructorException", TEST(DestructorExceptionTest::test))
    {
    }

    void test()
    {
        BedrockClusterTester tester;
        BedrockTester& leader = tester.getTester(0);

        // Wait for the cluster to be ready.
        ASSERT_TRUE(tester.getTester(0).waitForState("LEADING"));
        ASSERT_TRUE(tester.getTester(1).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester.getTester(2).waitForState("FOLLOWING"));

        // Send a command that throws in its destructor. The server should catch the exception
        // and still return 200 OK.
        SData cmd("throwindestruction");
        auto result = leader.executeWaitVerifyContent(cmd, "200 OK");

        // Verify the server is still functional by sending a follow-up command.
        SData followUp("testcommand");
        leader.executeWaitVerifyContent(followUp, "200 OK");
    }
} __DestructorExceptionTest;
