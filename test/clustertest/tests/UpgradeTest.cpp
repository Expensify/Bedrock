#include <libstuff/SData.h>
#include <libstuff/SRandom.h>
#include <test/clustertest/BedrockClusterTester.h>

struct UpgradeTest : tpunit::TestFixture
{
    UpgradeTest()
        : tpunit::TestFixture("Upgrade",
                              TEST(UpgradeTest::mismatchedFollowerSendMultipleCommands)
        )
    {
    }

    void mismatchedFollowerSendMultipleCommands()
    {
        BedrockClusterTester tester;

        // Cluster is up, shut down follower 2.
        tester.stopNode(2);

        // Change it's version.
        tester.getTester(2).updateArgs({{"-versionOverride", "FAKE_VERSION"}});

        // Start it back up and let it go following.
        tester.startNode(2);
        ASSERT_TRUE(tester.getTester(2).waitForState("FOLLOWING"));

        // Get status info from leader and follower.
        SData status("Status");
        auto results = tester.getTester(0).executeWaitMultipleData({status}, 1, false, true);
        string leaderVersion = SParseJSONObject(results[0].content)["version"];
        string leaderState = SParseJSONObject(results[0].content)["state"];
        results = tester.getTester(2).executeWaitMultipleData({status}, 1, false, true);
        string followerVersion = SParseJSONObject(results[0].content)["version"];
        string followerState = SParseJSONObject(results[0].content)["state"];

        // Verify it's what we expect.
        ASSERT_EQUAL(leaderState, "LEADING");
        ASSERT_EQUAL(followerState, "FOLLOWING");
        ASSERT_EQUAL(followerVersion, "FAKE_VERSION");
        ASSERT_NOT_EQUAL(leaderVersion, followerVersion);

        // Send two commands *on the same socket* (the `1` param to executeWaitMultipleData is number of sockets to
        // open) and verify we get results for both of them.
        SData idcollision1("idcollision");
        SData idcollision2("idcollision");
        results = tester.getTester(2).executeWaitMultipleData({idcollision1, idcollision2}, 1, false, true);
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        ASSERT_EQUAL(results[1].methodLine, "200 OK");
    }
} __UpgradeTest;
