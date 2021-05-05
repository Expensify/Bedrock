#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct VersionMismatchTest : tpunit::TestFixture {
    VersionMismatchTest()
        : tpunit::TestFixture("VersionMismatch", TEST(VersionMismatchTest::test)) { }

    void test()
    {
        // Create a cluster.
        BedrockClusterTester tester;

        // Restart one of the followers on a new version.
        tester.getTester(2).stopServer();
        tester.getTester(2).updateArgs({{"-versionOverride", "ABCDE"}});
        tester.getTester(2).startServer();

        // Send a query to all three and make sure the version-mismatched one escalates.
        // Can do them all in parallel so might as well.
        list<thread> threads;
        for (size_t i = 0; i < 3; i++) {
            threads.emplace_back([this, i, &tester](){
                SData command("Query");
                command["Query"] = "SELECT 1;";
                auto result = tester.getTester(i).executeWaitMultipleData({command})[0];

                // For read commands sent directly to leader, or to a follower on the same version as leader, there
                // should be no upstream times. However, on a follower on a different version to leader, it should
                // escalates even read commands.
                ASSERT_EQUAL(result.isSet("upstreamPeekTime"), i == 2);
            });
        }
        for (auto& t : threads) {
            t.join();
        }
    }
} __VersionMismatchTest;
