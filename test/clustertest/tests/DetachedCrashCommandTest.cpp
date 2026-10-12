#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct DetachedCrashCommandTest : tpunit::TestFixture
{
    DetachedCrashCommandTest()
        : tpunit::TestFixture("DetachedCrashCommand",
                              TEST(DetachedCrashCommandTest::testBlacklistSurvivesReattach))
    {
    }

    void testBlacklistSurvivesReattach()
    {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& follower = tester.getTester(1);
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));
        ASSERT_TRUE(follower.waitForStatusTerm("crashCommands", "0"));

        follower.executeWaitVerifyContent(SData("Detach"), "203 DETACHING", true);
        ASSERT_TRUE(follower.waitForStatusTerm("isDetached", "true"));

        // The control port remains available even after cluster activity has stopped.
        SData blacklistedRequest("OpenApp");
        blacklistedRequest["accountID"] = "12957057";
        SData crashCommand("CRASH_COMMAND");
        crashCommand.content = blacklistedRequest.serialize();
        follower.executeWaitVerifyContent(crashCommand, "200 OK", true);

        STable restriction;
        restriction["accountID"] = "12957057";
        STable entry;
        entry["OpenApp"] = SComposeJSONArray(vector<string>{SComposeJSONObject(restriction)});
        const string expectedBlacklist = SComposeJSONArray(vector<string>{SComposeJSONObject(entry)});

        STable detachedStatus = SParseJSONObject(follower.executeWaitVerifyContent(SData("Status"), "200 OK", true));
        ASSERT_EQUAL(detachedStatus["isDetached"], "true");
        ASSERT_EQUAL(detachedStatus["crashCommands"], "1");
        ASSERT_EQUAL(detachedStatus["crashCommandList"], expectedBlacklist);

        follower.executeWaitVerifyContent(SData("Attach"), "204 ATTACHING", true);
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));
        ASSERT_TRUE(follower.waitForStatusTerm("isDetached", "false"));

        STable attachedStatus = SParseJSONObject(follower.executeWaitVerifyContent(SData("Status"), "200 OK", true));
        ASSERT_EQUAL(attachedStatus["crashCommands"], "1");
        ASSERT_EQUAL(attachedStatus["crashCommandList"], expectedBlacklist);
    }
} __DetachedCrashCommandTest;
