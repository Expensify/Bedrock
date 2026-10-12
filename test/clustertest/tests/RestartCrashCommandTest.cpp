#include <libstuff/SData.h>
#include <libstuff/STCPManager.h>
#include <test/clustertest/BedrockClusterTester.h>

struct RestartCrashCommandTest : tpunit::TestFixture
{
    RestartCrashCommandTest()
        : tpunit::TestFixture("RestartCrashCommand",
                              TEST(RestartCrashCommandTest::testBlacklistResyncAfterRestart),
                              TEST(RestartCrashCommandTest::testBlacklistResyncWithoutHTTP))
    {
    }

    void testBlacklistResyncAfterRestart()
    {
        testBlacklistResync(false);
    }

    void testBlacklistResyncWithoutHTTP()
    {
        testBlacklistResync(true);
    }

    void testBlacklistResync(bool blockHTTP)
    {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& follower = tester.getTester(1);
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));

        SData blacklistedRequest("OpenApp");
        blacklistedRequest["accountID"] = "12957057";
        SData crashCommand("CRASH_COMMAND");
        crashCommand.content = blacklistedRequest.serialize();

        STable restriction;
        restriction["accountID"] = "12957057";
        STable entry;
        entry["OpenApp"] = SComposeJSONArray(vector<string>{SComposeJSONObject(restriction)});
        const string expectedBlacklist = SComposeJSONArray(vector<string>{SComposeJSONObject(entry)});

        for (size_t i : {0, 1, 2}) {
            BedrockTester& node = tester.getTester(i);
            node.executeWaitVerifyContent(crashCommand, "200 OK", true);
            STable status = SParseJSONObject(node.executeWaitVerifyContent(SData("Status"), "200 OK", true));
            ASSERT_EQUAL(status["crashCommands"], "1");
            ASSERT_EQUAL(status["crashCommandList"], expectedBlacklist);
        }

        // Unlike detach/attach, a process restart loses the local in-memory blacklist.
        tester.stopNode(1);

        // Occupy the private port without accepting requests to prove recovery does not depend on HTTP replay.
        STCPManager socketManager;
        unique_ptr<STCPManager::Port> blockedHTTP;
        if (blockHTTP) {
            blockedHTTP = socketManager.openPort(follower.getArg("-commandPortPrivate"));
            ASSERT_TRUE(blockedHTTP);
        }

        tester.startNode(1);
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));

        // Allow asynchronous login-time replay to finish before checking recovery.
        follower.waitForStatusTerm("crashCommands", "1", 10'000'000);
        STable status = SParseJSONObject(follower.executeWaitVerifyContent(SData("Status"), "200 OK", true));
        ASSERT_EQUAL(status["crashCommands"], "1");
        ASSERT_EQUAL(status["crashCommandList"], expectedBlacklist);
    }
} __RestartCrashCommandTest;
