#include <sys/stat.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ClusterUpgradeTest : tpunit::TestFixture
{
    ClusterUpgradeTest()
        : tpunit::TestFixture("ClusterUpgrade",
                              BEFORE_CLASS(ClusterUpgradeTest::setup),
                              AFTER_CLASS(ClusterUpgradeTest::teardown),
                              TEST(ClusterUpgradeTest::test)
        )
    {
    }

    BedrockClusterTester* tester;
    list<string> writtenValues;
    string prodBedrockName;
    string prodBedrockPluginName;
    string newTestPlugin;

    void setup()
    {
        // Get the most recent releases.
        const size_t RECENT_RELEASES_TO_CHECK = 5;

        // Tags are somehow missing in Travis, so we'll fetch them.
        ASSERT_EQUAL(system("git fetch --all --tags > /dev/null"), 0);

        // In theory, we should look at releases, not tags, but we don't have hat without github API access. But, since we only ever create tags for releases, we can look at recent tags
        // instead and get the same information.
        const string tempFile = "brdata.txt";
        const string command = "git tag --sort=-committerdate | head -n" + to_string(RECENT_RELEASES_TO_CHECK) + " > " + tempFile;
        ASSERT_EQUAL(system(command.c_str()), 0);
        string data = SFileLoad(tempFile);
        SFileDelete(tempFile);
        list<string> tagNames = SParseList(data, '\n');

        // Now choose the one to use. We want to test against the most recent release that isn't the commit we're currently on.
        // the commit number of the tag: git rev-list -n 1 $TAG
        // The commit number we're currently on: git rev-parse HEAD
        // If the current commit matches the tested tag, the script returns 1 and we check the next one. When the script returns 0, that's the release we'll use.
        string bedrockTagName;
        for (const auto& tagName : tagNames) {
            string checkIfOnLatestTag = "/bin/bash -c 'if [[ \"$(git rev-list -n 1 " + tagName + ")\" == \"$(git rev-parse HEAD)\" ]]; then exit 1; else exit 0; fi'";
            int result = system(checkIfOnLatestTag.c_str());
            if (result == 0) {
                bedrockTagName = tagName;
                break;
            }
        }

        // Make sure we got something to test.
        ASSERT_NOT_EQUAL(bedrockTagName, "");

        // If you'd like to test against a particular tag, uncomment the following line. The value chosen here was a
        // known bad version that failed to escalate commands at upgrade when first deployed.
        // bedrockTagName = "2022-05-06";

        // If we've already built this, don't bother doing it again. This makes running this test multiple times in a
        // row much faster.
        string prodBedrockDirName = "/tmp/bedrock-" + bedrockTagName;
        prodBedrockName = prodBedrockDirName + "/bedrock";
        prodBedrockPluginName = prodBedrockDirName + "/testplugin.so";
        if (!SFileExists(prodBedrockName)) {
            // Get a directory we can work in.
            char brReleaseDirArr[] = "/tmp/br-prod-test-XXXXXX";
            ASSERT_EQUAL(mkdtemp(brReleaseDirArr), brReleaseDirArr);
            string brReleaseDir(brReleaseDirArr, sizeof(brReleaseDirArr) - 1);

            // Clone bedrock.
            ASSERT_FALSE(system(("cd " + brReleaseDir + " && git clone https://github.com/Expensify/Bedrock.git > /dev/null").c_str()));

            // Check out the release tag.
            ASSERT_FALSE(system(("cd " + brReleaseDir + " && cd Bedrock && git checkout " + bedrockTagName + "  > /dev/null").c_str()));

            // Build the release.
            ASSERT_FALSE(system(("cd " + brReleaseDir + " && cd Bedrock && CXX=clang++-18 CC=clang-18 make -j8 > /dev/null").c_str()));

            // Save the final product.
            mkdir(prodBedrockDirName.c_str(), 0755);
            ASSERT_FALSE(system(("mv " + brReleaseDir + "/Bedrock/bedrock " + prodBedrockName).c_str()));
            ASSERT_FALSE(system(("mv " + brReleaseDir + "/Bedrock/test/clustertest/testplugin/testplugin.so " + prodBedrockPluginName).c_str()));

            // Remove the intermediate dir.
            rmdir(brReleaseDir.c_str());
        }

        // Figure out where the new test plugin is.
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }
        newTestPlugin = string(cwd) + "/testplugin/testplugin.so";

        // Load the whole prod cluster with the prod test plugin.
        tester = new BedrockClusterTester("db," + prodBedrockPluginName, prodBedrockName);
    }

    void teardown()
    {
        delete tester;
    }

    vector<string> getVersions()
    {
        SData status("Status");
        vector<string> versions(3);
        for (auto i: {0, 1, 2}) {
            vector<SData> statusResult = tester->getTester(i).executeWaitMultipleData({status});
            versions[i] = SParseJSONObject(statusResult[0].content)["version"];
        }
        return versions;
    }

    uint64_t getCommitCount(int node)
    {
        SData status("Status");
        vector<SData> statusResult = tester->getTester(node).executeWaitMultipleData({status});
        return SToUInt64(SParseJSONObject(statusResult[0].content)["CommitCount"]);
    }

    // Writes a row from `writeToNode` and waits for every running node to reach the resulting commit. Checking
    // states and versions says the cluster looks healthy; this says it's actually replicating. A node that quietly
    // stopped applying leader's transactions looks identical to a healthy one until someone checks.
    //
    // The rows themselves are checked at the end of the test, by which point every node is on the same version. We
    // can't check them here: a follower running a different version than leader forwards commands to a node that
    // matches leader, so a read sent to it during the rolling restart is answered from somebody else's database.
    void verifyReplication(const string& value, int writeToNode, const vector<int>& runningNodes)
    {
        // A write sent to a follower escalates to leader, so this works from any node.
        SData cmd("idcollision");
        cmd["value"] = value;
        vector<SData> result = tester->getTester(writeToNode).executeWaitMultipleData({cmd});
        ASSERT_EQUAL(result[0].methodLine, "200 OK");
        writtenValues.push_back(value);

        // Followers replicate asynchronously, so wait for each of them to reach the commit we just made. The highest
        // count in the cluster belongs to the leader, which is the node that just committed. `Status` is answered by
        // whichever node we ask, even one running a different version, so these counts are each node's own.
        uint64_t leaderCommitCount = 0;
        for (int node : runningNodes) {
            leaderCommitCount = max(leaderCommitCount, getCommitCount(node));
        }
        for (int node : runningNodes) {
            ASSERT_TRUE(tester->getTester(node).waitForStatusTerm("CommitCount", to_string(leaderCommitCount)));
        }
    }

    void test()
    {
        // Let the entire cluster come up on the production version.
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // Get the versions from the cluster.
        auto versions = getVersions();

        // Save the production version for later comparison.
        string prodVersion = versions[0];

        // Verify all three are the same.
        ASSERT_EQUAL(versions[0], versions[1]);
        ASSERT_EQUAL(versions[0], versions[2]);

        // Baseline: the cluster replicates before we change anything.
        verifyReplication("all nodes on production", 0, {0, 1, 2});

        // Restart 2 on the new version.
        tester->getTester(2).stopServer();
        tester->getTester(2).serverName = "bedrock";
        tester->getTester(2).updateArgs({{"-plugins", "db," + newTestPlugin}});
        tester->getTester(2).startServer();
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // Verify the server has been upgraded and the version is different.
        versions = getVersions();
        string devVersion = versions[2];
        ASSERT_NOT_EQUAL(prodVersion, devVersion);

        // Send a write command on 2 and verify it reaches every node. This verifies that we can escalate from
        // new->old, and that the upgraded follower still receives what the old leader replicates.
        verifyReplication("new follower escalating to old leader", 2, {0, 1, 2});

        // Now we shut down the old leader. This makes the remaining old follower become leader.
        tester->getTester(0).stopServer();

        // We should now have a two-node cluster with 1 leading and 2 following.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // The old node is leading the upgraded one now, with the third node down. Write from the leader: a follower
        // running a different version than leader forwards commands to a follower that matches leader instead of
        // handling them, and with only two nodes up there's no such node to forward to.
        verifyReplication("old leader, new follower", 1, {1, 2});

        // Start up the old leader on the new version.
        tester->getTester(0).serverName = "bedrock";
        tester->getTester(0).updateArgs({{"-plugins", "db," + newTestPlugin}});
        tester->getTester(0).startServer();

        // We should get the expected cluster state.
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // Now 0 and 2 are the new version, and 1 is the old version.
        versions = getVersions();
        ASSERT_EQUAL(versions[0], devVersion);
        ASSERT_EQUAL(versions[1], prodVersion);
        ASSERT_EQUAL(versions[2], devVersion);

        // Now we need to send a command to node 1 to verify we can escalate old->new, and that the node still on the
        // old version receives what the upgraded leader replicates.
        verifyReplication("old follower escalating to new leader", 1, {0, 1, 2});

        // And finally, upgrade the last node.
        tester->getTester(1).stopServer();
        tester->getTester(1).serverName = "bedrock";
        tester->getTester(1).updateArgs({{"-plugins", "db," + newTestPlugin}});
        tester->getTester(1).startServer();
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));

        // And verify everything is upgraded.
        versions = getVersions();
        ASSERT_EQUAL(versions[0], devVersion);
        ASSERT_EQUAL(versions[1], devVersion);
        ASSERT_EQUAL(versions[2], devVersion);

        // And that the fully upgraded cluster still replicates.
        verifyReplication("all nodes upgraded", 0, {0, 1, 2});

        // Every node runs the same version now, so a read is answered by the node we send it to rather than being
        // forwarded to one matching leader. Check that every row written during the rolling restart is on every node,
        // including the ones committed while node 0 was down and had to be synchronized after it came back.
        for (const string& value : writtenValues) {
            for (auto i : {0, 1, 2}) {
                ASSERT_EQUAL(tester->getTester(i).readDB("SELECT COUNT(*) FROM test WHERE value = " + SQ(value) + ";"), "1");
            }
        }
    }
} __ClusterUpgradeTest;
