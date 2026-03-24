#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ReloadPluginTest : tpunit::TestFixture {
    ReloadPluginTest()
        : tpunit::TestFixture("ReloadPlugin",
                              TEST(ReloadPluginTest::happyPathReload),
                              TEST(ReloadPluginTest::reloadDrainsInflightCommands),
                              TEST(ReloadPluginTest::reloadBuiltinPluginRejected),
                              TEST(ReloadPluginTest::reloadNonexistentPluginRejected),
                              TEST(ReloadPluginTest::reloadPreservesDBState),
                              TEST(ReloadPluginTest::reloadOnFollower),
                              TEST(ReloadPluginTest::doubleReloadSerializes))
    {
    }

    // Helper to send ReloadPlugin control command
    string sendReloadPlugin(BedrockTester& tester, const string& pluginName) {
        SData command("ReloadPlugin");
        command["PluginName"] = pluginName;
        return tester.executeWaitVerifyContent(command, "", true);
    }

    void happyPathReload() {
        // Start a 3-node cluster with the test plugin
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& leader = tester.getTester(0);

        // Verify plugin works before reload
        SData cmd("testcommand");
        leader.executeWaitVerifyContent(cmd, "200");

        // Reload the plugin
        SData reloadCmd("ReloadPlugin");
        reloadCmd["PluginName"] = "TESTPLUGIN";
        leader.executeWaitVerifyContent(reloadCmd, "200", true);

        // Verify plugin still works after reload
        SData cmd2("testcommand");
        leader.executeWaitVerifyContent(cmd2, "200");
    }

    void reloadDrainsInflightCommands() {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& leader = tester.getTester(0);

        // Start a slow query that will take a few seconds
        SData slowCmd("slowquery");
        slowCmd["size"] = "5000000";

        // Send the slow command in a background thread
        thread slowThread([&]() {
            leader.executeWaitVerifyContent(slowCmd, "200");
        });

        // Give it a moment to start processing
        usleep(100'000);

        // Send reload - it should wait for the slow query to finish
        SData reloadCmd("ReloadPlugin");
        reloadCmd["PluginName"] = "TESTPLUGIN";
        leader.executeWaitVerifyContent(reloadCmd, "200", true);

        slowThread.join();

        // Verify commands work after reload
        SData cmd("testcommand");
        leader.executeWaitVerifyContent(cmd, "200");
    }

    void reloadBuiltinPluginRejected() {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& leader = tester.getTester(0);

        SData cmd("ReloadPlugin");
        cmd["PluginName"] = "DB";
        leader.executeWaitVerifyContent(cmd, "400", true);
    }

    void reloadNonexistentPluginRejected() {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& leader = tester.getTester(0);

        SData cmd("ReloadPlugin");
        cmd["PluginName"] = "FAKEPLUGIN";
        leader.executeWaitVerifyContent(cmd, "400", true);
    }

    void reloadPreservesDBState() {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& leader = tester.getTester(0);

        // Insert some data
        SData insertCmd("testquery");
        insertCmd["Query"] = "INSERT INTO test (id, value) VALUES (12345, 'reload_test_data');";
        leader.executeWaitVerifyContent(insertCmd, "200");

        // Reload the plugin
        SData reloadCmd("ReloadPlugin");
        reloadCmd["PluginName"] = "TESTPLUGIN";
        leader.executeWaitVerifyContent(reloadCmd, "200", true);

        // Verify data persists after reload
        SData selectCmd("testquery");
        selectCmd["Query"] = "SELECT value FROM test WHERE id = 12345;";
        leader.executeWaitVerifyContent(selectCmd, "200");
    }

    void reloadOnFollower() {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& follower = tester.getTester(1);

        // Wait for the follower to be in FOLLOWING state
        follower.waitForState("FOLLOWING");

        // Reload on the follower
        SData reloadCmd("ReloadPlugin");
        reloadCmd["PluginName"] = "TESTPLUGIN";
        follower.executeWaitVerifyContent(reloadCmd, "200", true);

        // Verify commands still work on the follower
        SData cmd("testcommand");
        follower.executeWaitVerifyContent(cmd, "200");
    }

    void doubleReloadSerializes() {
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& leader = tester.getTester(0);

        // Send two reload commands concurrently
        thread t1([&]() {
            SData reloadCmd("ReloadPlugin");
            reloadCmd["PluginName"] = "TESTPLUGIN";
            leader.executeWaitVerifyContent(reloadCmd, "", true);
        });
        thread t2([&]() {
            SData reloadCmd("ReloadPlugin");
            reloadCmd["PluginName"] = "TESTPLUGIN";
            leader.executeWaitVerifyContent(reloadCmd, "", true);
        });

        t1.join();
        t2.join();

        // Server should still be stable
        SData cmd("testcommand");
        leader.executeWaitVerifyContent(cmd, "200");
    }

} __ReloadPluginTest;
