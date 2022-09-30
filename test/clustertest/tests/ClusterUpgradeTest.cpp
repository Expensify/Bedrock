#include <sys/stat.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ClusterUpgradeTest : tpunit::TestFixture {
    ClusterUpgradeTest()
        : tpunit::TestFixture("ClusterUpgrade",
                              BEFORE_CLASS(ClusterUpgradeTest::setup),
                              AFTER_CLASS(ClusterUpgradeTest::teardown),
                              TEST(ClusterUpgradeTest::test)
                             ) { }

    BedrockClusterTester* tester;
    string prodBedrockName;
    string prodBedrockPluginName;
    string newTestPlugin;

    void setup() {
    cout << "Setup A" << endl;
        // Get the most recent releases.
        const size_t RECENT_RELEASES_TO_CHECK = 5;

        // In theory, we should look at releases, not tags, but we don't have hat without github API access. But, since we only ever create tags for releases, we can look at recent tags
        // instead and get the same information. We shouldn't need to look at remote tags, we're only interested in being able to upgrade to the version we're building, which should be at
        // the head of the tree here, and contain all older tags.
        const string tempFile = "brdata.txt";
        const string command = "git tag --sort=-committerdate | head -n" + to_string(RECENT_RELEASES_TO_CHECK) + " > " + tempFile;
        ASSERT_EQUAL(system(command.c_str()), 0);
        string data = SFileLoad(tempFile);
        SFileDelete(tempFile);
        cout << "Loaded tags: " << endl;
        cout << data << endl;
        list<string> tagNames = SParseList(data, '\n');

    cout << "Setup B" << endl;
        // Now choose the one to use. We want to test against the most recent release that isn't the commit we're currently on.
        // the commit number of the tag: git rev-list -n 1 $TAG
        // The commit number we're currently on: git rev-parse HEAD
        // If the current commit matches the tested tag, the script returns 1 and we check the next one. When the script returns 0, that's the release we'll use.
        string bedrockTagName;
        for (const auto& tagName : tagNames) {
            string checkIfOnLatestTag = "/bin/bash -c 'if [[ \"$(git rev-list -n 1 " + tagName + ")\" == \"$(git rev-parse HEAD)\" ]]; then exit 1; else exit 0; fi'";
            int result = system(checkIfOnLatestTag.c_str());
                cout << checkIfOnLatestTag << endl;
            if (result == 0) {
                cout << "Got BR name: " << bedrockTagName << endl;
                bedrockTagName = tagName;
                break;
            }
        }
        if (bedrockTagName.empty()) {
            cout << "Couldn't find a tag!!!!" << endl;
        }

    cout << "Setup C" << endl;
        // Make sure we got something to test.
        ASSERT_NOT_EQUAL(bedrockTagName, "");

        // If you'd like to test against a particular tag, uncomment the following line. The value chosen here was a
        // known bad version that failed to escalate commands at upgrade when first deployed.
        // bedrockTagName = "2022-05-06";

    cout << "Setup D" << endl;
        // If we've already built this, don't bother doing it again. This makes running this test multiple times in a
        // row much faster.
        string prodBedrockDirName = "/tmp/bedrock-" + bedrockTagName;
        prodBedrockName = prodBedrockDirName + "/bedrock";
        prodBedrockPluginName = prodBedrockDirName + "/testplugin.so";
    cout << "Setup E" << endl;
        if (!SFileExists(prodBedrockName)) {
            // Get a directory we can work in.
            char brReleaseDirArr[] = "/tmp/br-prod-test-XXXXXX";
            ASSERT_EQUAL(mkdtemp(brReleaseDirArr), brReleaseDirArr);
            string brReleaseDir(brReleaseDirArr, sizeof(brReleaseDirArr) - 1);

    cout << "Setup F" << endl;
            // Clone bedrock.
            ASSERT_FALSE(system(("cd " + brReleaseDir + " && git clone https://github.com/Expensify/Bedrock.git > /dev/null").c_str()));

    cout << "Setup G" << endl;
            // Check out the release tag.
            ASSERT_FALSE(system(("cd " + brReleaseDir + " && cd Bedrock && git checkout " + bedrockTagName + "  > /dev/null").c_str()));

    cout << "Setup H" << endl;
            // Build the release.
            ASSERT_FALSE(system(("cd " + brReleaseDir + " && cd Bedrock && make -j8 > /dev/null").c_str()));

    cout << "Setup I" << endl;
            // Save the final product.
            mkdir(prodBedrockDirName.c_str(), 0755);
            ASSERT_FALSE(system(("mv " + brReleaseDir + "/Bedrock/bedrock " + prodBedrockName).c_str()));
            ASSERT_FALSE(system(("mv " + brReleaseDir + "/Bedrock/test/clustertest/testplugin/testplugin.so " + prodBedrockPluginName).c_str()));

    cout << "Setup J" << endl;
            // Remove the intermediate dir.
            rmdir(brReleaseDir.c_str());
        }

    cout << "Setup K" << endl;
        // Figure out where the new test plugin is.
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }
        newTestPlugin = string(cwd) + "/testplugin/testplugin.so";

    cout << "Setup L" << endl;
        // Load the whole prod cluster with the prod test plugin.
        cout << "Loading prod cluster " << prodBedrockPluginName << ", " << prodBedrockName << endl;
        tester = new BedrockClusterTester(prodBedrockPluginName, prodBedrockName);
    cout << "Setup M" << endl;
    }

    void teardown() {
        delete tester;
    }

    vector<string> getVersions() {
        SData status("Status");
        vector<string> versions(3);
        for (auto i: {0, 1, 2}) {
            vector<SData> statusResult = tester->getTester(i).executeWaitMultipleData({status});
            versions[i] = SParseJSONObject(statusResult[0].content)["version"];
        }
        return versions;
    }

    void test() {
    cout << "A" << endl;
        // Let the entire cluster come up on the production version.
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));
    cout << "A.1" << endl;
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

    cout << "B" << endl;
        // Get the versions from the cluster.
        auto versions = getVersions();

        // Save the production version for later comparison.
        string prodVersion = versions[0];

    cout << "C" << endl;
        // Verify all three are the same.
        ASSERT_EQUAL(versions[0], versions[1]);
        ASSERT_EQUAL(versions[0], versions[2]);

    cout << "D" << endl;
        // Restart 2 on the new version.
        tester->getTester(2).stopServer();
        tester->getTester(2).serverName = "bedrock";
        tester->getTester(2).updateArgs({{"-plugins", newTestPlugin}});
        tester->getTester(2).startServer();
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

    cout << "E" << endl;
        // Verify the server has been upgraded and the version is different.
        versions = getVersions();
        string devVersion = versions[2];
        ASSERT_NOT_EQUAL(prodVersion, devVersion);

    cout << "F" << endl;
        // Send a write command on 2 and verify we get a reasonable response. This should verify that we can escalate from new->old.
        SData cmd("idcollision");
        vector<SData> cmdResult = tester->getTester(2).executeWaitMultipleData({cmd});
        ASSERT_EQUAL(cmdResult[0].methodLine, "200 OK");

    cout << "G" << endl;
        // Now we shut down the old leader. This makes the remaining old follower become leader.
        tester->getTester(0).stopServer();

    cout << "H" << endl;
        // We should now have a two-node cluster with 1 leading and 2 following.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

    cout << "I" << endl;
        // Start up the old leader on the new version.
        tester->getTester(0).serverName = "bedrock";
        tester->getTester(0).updateArgs({{"-plugins", newTestPlugin}});
        tester->getTester(0).startServer();

    cout << "J" << endl;
        // We should get the expected cluster state.
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

    cout << "K" << endl;
        // Now 0 and 2 are the new version, and 1 is the old version.
        versions = getVersions();
        ASSERT_EQUAL(versions[0], devVersion);
        ASSERT_EQUAL(versions[1], prodVersion);
        ASSERT_EQUAL(versions[2], devVersion);

    cout << "L" << endl;
        // Now we need to send a command to node 1 to verify we can escalate old->new.
        cmdResult = tester->getTester(1).executeWaitMultipleData({cmd});
        ASSERT_EQUAL(cmdResult[0].methodLine, "200 OK");

    cout << "M" << endl;
        // And finally, upgrade the last node.
        tester->getTester(1).stopServer();
        tester->getTester(1).serverName = "bedrock";
        tester->getTester(1).updateArgs({{"-plugins", newTestPlugin}});
        tester->getTester(1).startServer();
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));

    cout << "N" << endl;
        // And verify everything is upgraded.
        versions = getVersions();
        ASSERT_EQUAL(versions[0], devVersion);
        ASSERT_EQUAL(versions[1], devVersion);
        ASSERT_EQUAL(versions[2], devVersion);
    }

} __ClusterUpgradeTest;
