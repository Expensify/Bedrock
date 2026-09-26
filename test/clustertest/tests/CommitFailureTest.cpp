#include <libstuff/SData.h>
#include <sys/wait.h>
#include <test/clustertest/BedrockClusterTester.h>

struct CommitFailureTest : tpunit::TestFixture
{
    CommitFailureTest()
        : tpunit::TestFixture("CommitFailure", TEST(CommitFailureTest::unexpectedCommitErrorIsFatal))
    {
    }

    void unexpectedCommitErrorIsFatal()
    {
        BedrockClusterTester cluster(ClusterSize::ONE_NODE_CLUSTER);
        BedrockTester& node = cluster.getTester(0);
        ASSERT_TRUE(node.waitForState("LEADING"));
        node.executeWaitMultipleData({SData("failcommit")}, 1, false, true);
        int status = 0;
        ASSERT_TRUE(node.waitForExit(status));
        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQUAL(WTERMSIG(status), SIGABRT);

        node.startServer();
        ASSERT_TRUE(node.waitForState("LEADING"));
        EXPECT_EQUAL(node.readDB("SELECT COUNT(*) FROM test WHERE id = 876570000;"), "0");
    }
} __CommitFailureTest;
