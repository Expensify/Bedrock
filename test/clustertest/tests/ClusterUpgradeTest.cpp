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

    void setup() {
        // Get the two most recent releases.
        const string tempJson = "brdata.json";
        string command = "curl --silent 'https://api.github.com/repos/Expensify/Bedrock/releases?page=1&per_page=2' -o " + tempJson;
        ASSERT_FALSE(system(command.c_str()));

        // Decide whether we want to test against the most recent tagged release, or the second most recent.
        // If we are doing this as part of a deploy, the most recent tagged release is this one, and we want to test against the previous one. Otherwise, if we are in a regular un-released
        // branch (for a typical Travis build) we want to test against the most recent release.
        // Parse a tag from it.
        string data = SFileLoad(tempJson);
        SFileDelete(tempJson);
        string bedrockTagName;
        list<string> j1 = SParseJSONArray(STrim(data));

        // There should be two releases.
        ASSERT_EQUAL(j1.size(), 2);

        // Pull the tag names from the JSON.
        array<string, 2> tagNames;
        for (size_t i = 0; i < 1; i++) {
            STable j2 = SParseJSONObject(j1.front());
            auto tag = j2.find("tag_name");
            if (tag != j2.end()) {
                tagNames[i] = tag->second;
            }
            if (j1.size()) {
                j1.pop_front();
            }
        }

        // Now choose the one to use.
        // the commit number of the tag: git rev-list -n 1 $TAG
        // The commit number we're currently on: git rev-parse HEAD
        // If the current commit matches the latest tag commit, our script returns 1 and we will test against the second tag. Otherwise, it returns 0 and we test against the first tag.
        string checkIfOnLatestTag = "/bin/bash -c 'if [ \"`git rev-list -n 1 " + tagNames[0] + "`\" = \"`git rev-parse HEAD`\" ]; then exit 1; else exit 0; fi'";
        int result = system(checkIfOnLatestTag.c_str());
        bedrockTagName = tagNames[result];

        cout << tagNames[0] << endl;
        cout << tagNames[1] << endl;
        cout << "Testing against: " << bedrockTagName << endl;

        // If you'd like to test against a particular tag, uncomment the following line. The value chosen here was a
        // known bad version that failed to escalate commands at upgrade when first deployed.
        // bedrockTagName = "2022-05-06";

        // If we've already built this, don't bother doing it again. This makes running this test multiple times in a
        // row much faster.
        prodBedrockName = "/tmp/bedrock-" + bedrockTagName; 
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
            ASSERT_FALSE(system(("cd " + brReleaseDir + " && cd Bedrock && make -j8 bedrock  > /dev/null").c_str()));

            // Save the final product.
            ASSERT_FALSE(system(("mv " + brReleaseDir + "/Bedrock/bedrock " + prodBedrockName).c_str()));
        }

        tester = new BedrockClusterTester("db,cache,jobs", prodBedrockName);
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

        // Restart 2 on the new version.
        tester->getTester(2).stopServer();
        tester->getTester(2).serverName = "bedrock";
        tester->getTester(2).startServer();
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // Verify the server has been upgraded and the version is different.
        versions = getVersions();
        string devVersion = versions[2];
        ASSERT_NOT_EQUAL(prodVersion, devVersion);

        // Send a write command on 2 and verify we get a reasonable response. This should verify that we can escalate from new->old.
        SData cmd("idcollision");
        vector<SData> cmdResult = tester->getTester(2).executeWaitMultipleData({cmd});
        ASSERT_EQUAL(cmdResult[0].methodLine, "200 OK");

        // Now we shut down the old leader. This makes the remaining old follower become leader.
        tester->getTester(0).stopServer();

        // We should now have a two-node cluster with 1 leading and 2 following.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // Start up the old leader on the new version.
        tester->getTester(0).serverName = "bedrock";
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

        // Now we need to send a command to node 1 to verify we can escalate old->new.
        cmdResult = tester->getTester(1).executeWaitMultipleData({cmd});
        ASSERT_EQUAL(cmdResult[0].methodLine, "200 OK");

        // And finally, upgrade the last node.
        tester->getTester(1).stopServer();
        tester->getTester(1).serverName = "bedrock";
        tester->getTester(1).startServer();
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));

        // And verify everything is upgraded.
        versions = getVersions();
        ASSERT_EQUAL(versions[0], devVersion);
        ASSERT_EQUAL(versions[1], devVersion);
        ASSERT_EQUAL(versions[2], devVersion);
    }

} __ClusterUpgradeTest;
