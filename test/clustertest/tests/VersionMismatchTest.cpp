#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct VersionMismatchTest : tpunit::TestFixture {
    VersionMismatchTest()
        : tpunit::TestFixture("VersionMismatch", 
            BEFORE_CLASS(VersionMismatchTest::setup),
            TEST(VersionMismatchTest::testReadEscalation), 
            TEST(VersionMismatchTest::testWriteEscalation),
            AFTER_CLASS(VersionMismatchTest::setup)) { }

    BedrockClusterTester* tester = nullptr;

    void setup() { 
        tester = new BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"});
        // Restart one of the followers on a new version.
        tester->getTester(2).stopServer();
        tester->getTester(2).updateArgs({{"-versionOverride", "ABCDE"}});
        tester->getTester(2).startServer();

        // Restart one of the followers on a new version.
        tester->getTester(4).stopServer();
        tester->getTester(4).updateArgs({{"-versionOverride", "ABCDE"}});
        tester->getTester(4).startServer();
    }
    void destroy() {
        delete tester;
    }
    void testReadEscalation()
    {
        // Send a query to all three and make sure the version-mismatched one escalates.
        for (size_t i = 0; i < 5; i++) {
            SData command("Query");
            command["Query"] = "SELECT 1;";
            auto result = tester->getTester(i).executeWaitMultipleData({command})[0];

            // For read commands sent directly to leader, or to a follower on the same version as leader, there
            // should be no upstream times. However, on a follower on a different version to leader, it should
            // escalates even read commands.
            if (i == 2){
                ASSERT_EQUAL(result["nodeNames"], "cluster_node_2,cluster_node_1");
            }
            if (i == 5){
                
                ASSERT_EQUAL(result["nodeNames"], "cluster_node_5,cluster_node_1");
            }
        }
    }
    void testWriteEscalation()
    {
        // Restart one of the followers on a new version.
        tester->getTester(2).stopServer();
        tester->getTester(2).updateArgs({{"-versionOverride", "ABCDE"}});
        tester->getTester(2).startServer();

        for (int64_t i = 0; i < 5; i++) {
            SData command("Query");
            command["Query"] = "INSERT INTO test VALUES(" + SQ(i) + ", " + SQ("val") + ");";
            auto result = tester->getTester(i).executeWaitMultipleData({command})[0];

            // For read commands sent directly to leader, or to a follower on the same version as leader, there
            // should be no upstream times. However, on a follower on a different version to leader, it should
            // escalates even read commands.
            if (i == 0){
                ASSERT_EQUAL(result["nodeNames"], "cluster_node_0");
            }
            if (i == 1){
                ASSERT_EQUAL(result["nodeNames"], "cluster_node_1,cluster_node_0");
            }
            if (i == 2){
                ASSERT_TRUE(SEndsWith(result["nodeNames"], "cluster_node_0"));

                // Since the follower selection is ramdon, there's no way to guarantee which server will
                // be the one in the middle. So let's just confirm that the string size is enough to do
                // only 3 servers in the path.
                // length: cluster_node_2,cluster_node_3,cluster_node_0 = 44
                ASSERT_EQUAL(result["nodeNames"].length(), 44);
            }
            if (i == 3){
                ASSERT_EQUAL(result["nodeNames"], "cluster_node_3,cluster_node_0");
            }
            if (i == 4) {
                ASSERT_TRUE(SEndsWith(result["nodeNames"], "cluster_node_0"));
                // Since the follower selection is ramdon, there's no way to guarantee which server will
                // be the one in the middle. So let's just confirm that the string size is enough to do
                // only 3 servers in the path.
                // length: cluster_node_4,cluster_node_3,cluster_node_0 = 44
                ASSERT_EQUAL(result["nodeNames"].length(), 44);
            }
            if (i == 5){
                ASSERT_EQUAL(result["nodeNames"], "cluster_node_5,cluster_node_0");
            }
        }
    }
} __VersionMismatchTest;
