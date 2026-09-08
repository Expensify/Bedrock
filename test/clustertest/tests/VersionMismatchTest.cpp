/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    VersionMismatchTest.cpp
 * Path:    test/clustertest/tests/VersionMismatchTest.cpp
 *
 * INTENT
 *   Cluster test verifying that a follower running a different version than
 *   the leader escalates every command it receives, reads included, rather
 *   than executing reads locally the way a same-version follower would.
 *
 * OBJECTS
 *   VersionMismatchTest                        - tpunit fixture; builds a 5-node cluster, bumps two
 *                                                 followers to a fake version via -versionOverride.
 *   VersionMismatchTest::testReadEscalation    - confirms the mismatched followers escalate reads.
 *   VersionMismatchTest::testWriteEscalation   - confirms writes always land on the leader regardless.
 *   __VersionMismatchTest                      - static instance that registers the fixture with tpunit.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with sibling test files.
 * ─────────────────────────────────────────────────────────────────────*/
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct VersionMismatchTest : tpunit::TestFixture
{
    VersionMismatchTest()
        : tpunit::TestFixture("VersionMismatch",
                              BEFORE_CLASS(VersionMismatchTest::setup),
                              TEST(VersionMismatchTest::testReadEscalation),
                              TEST(VersionMismatchTest::testWriteEscalation),
                              AFTER_CLASS(VersionMismatchTest::teardown))
    {
    }

    BedrockClusterTester* tester = nullptr;

    void setup()
    {
        tester = new BedrockClusterTester(ClusterSize::FIVE_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"});
        // Restart one of the followers on a new version.
        tester->getTester(2).stopServer();
        tester->getTester(2).updateArgs({{"-versionOverride", "ABCDE"}});
        tester->getTester(2).startServer();

        // Restart one of the followers on a new version.
        tester->getTester(4).stopServer();
        tester->getTester(4).updateArgs({{"-versionOverride", "ABCDE"}});
        tester->getTester(4).startServer();
    }

    void teardown()
    {
        delete tester;
    }

    void testReadEscalation()
    {
        // Send a query to all three and make sure the version-mismatched one escalates.
        for (size_t i = 0; i < 5; i++) {
            SData command("testquery");
            command["Query"] = "SELECT 1;";
            auto result = tester->getTester(i).executeWaitMultipleData({command})[0];

            // For read commands sent directly to leader, or to a follower on the same version as leader, there
            // we don't care about how they are executed. However, on a follower on a different version to leader,
            // it should escalates even read commands to follower peers.
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
