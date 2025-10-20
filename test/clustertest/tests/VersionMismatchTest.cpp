#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct VersionMismatchTest : tpunit::TestFixture {
    VersionMismatchTest()
        : tpunit::TestFixture("VersionMismatch",
            BEFORE_CLASS(VersionMismatchTest::setup),
            TEST(VersionMismatchTest::testReadEscalation),
            TEST(VersionMismatchTest::testWriteEscalation),
            AFTER_CLASS(VersionMismatchTest::teardown)) { }

    BedrockClusterTester* tester = nullptr;

    void setup() {
        tester = new BedrockClusterTester(ClusterSize::FIVE_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"});

        // Restart two servers on a different version.
        thread t1([&](){
            tester->getTester(2).stopServer();
            tester->getTester(2).updateArgs({{"-versionOverride", "ABCDE"}});
            tester->getTester(2).startServer();
        });
        thread t2([&](){
            tester->getTester(4).stopServer();
            tester->getTester(4).updateArgs({{"-versionOverride", "ABCDE"}});
            tester->getTester(4).startServer();
        });
        t1.join();
        t2.join();
    }

    void teardown() {
        delete tester;
    }

    void testReadEscalation()
    {
        // Send a query to all five and make sure the version-mismatched ones escalates.
        for (size_t i = 0; i < 5; i++) {

            bool stateIsOK = tester->getTester(i).waitForLeadingFollowing();
            if (!stateIsOK) {
                cout << "Node " << i << " failed to reach leading or following." << endl;
            }

            SData command("testquery");
            command["Query"] = "SELECT 1;";
            auto result = tester->getTester(i).executeWaitMultipleData({command})[0];

            // For read commands sent directly to leader, or to a follower on the same version as leader, there
            // we don't care about how they are executed. However, on a follower on a different version to leader,
            // it should escalates even read commands to follower peers.
            if (result["nodeRequestWasExecuted"].length() == 0) {
                cout << "No nodeRequestWasExecuted from node " << i << endl;
                cout << result.serialize() << endl;
                cout << "DONE" << endl;
            }
            ASSERT_TRUE(result["nodeRequestWasExecuted"].length() > 0);
            if (i == 2 || i == 4) {
                // Confirm it didn't execute in leader
                ASSERT_NOT_EQUAL(result["nodeRequestWasExecuted"], "cluster_node_0");

                // Confirm it didn't execute in the server with version mismatch
                ASSERT_NOT_EQUAL(result["nodeRequestWasExecuted"], "cluster_node_" + to_string(i));
            }
        }
    }
    void testWriteEscalation()
    {
        for (int64_t i = 0; i < 5; i++) {
            SData command("testquery");
            command["Query"] = "INSERT INTO test VALUES(" + SQ(i) + ", " + SQ("val") + ");";
            auto result = tester->getTester(i).executeWaitMultipleData({command})[0];

            // For read commands sent directly to leader, or to a follower on the same version as leader, the one
            // that will final execute the request should always be the leader
            ASSERT_EQUAL(result["nodeRequestWasExecuted"], "cluster_node_0");
        }
    }
} __VersionMismatchTest;
