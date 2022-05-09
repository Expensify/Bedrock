#include <sys/stat.h>
#include <test/clustertest/BedrockClusterTester.h>

struct UpgradeTest : tpunit::TestFixture {
    UpgradeTest()
        : tpunit::TestFixture("Upgrade",
                              BEFORE_CLASS(UpgradeTest::setup),
                              AFTER_CLASS(UpgradeTest::teardown),
                              TEST(UpgradeTest::test)
                             ) { }

    BedrockClusterTester* tester;
    string prodBedrockName;

    void setup() {
        // Get the list of recent releases.
        const string tempJson = "brdata.json";
        string command = "curl --silent 'https://api.github.com/repos/Expensify/Bedrock/releases?page=1&per_page=1' -o " + tempJson;
        ASSERT_FALSE(system(command.c_str()));

        // Parse a tag from it.
        string data = SFileLoad(tempJson);
        SFileDelete(tempJson);
        string bedrockTagName;
        list<string> j1 = SParseJSONArray(STrim(data));
        if (j1.size()) {
            STable j2 = SParseJSONObject(j1.front());
            auto tag = j2.find("tag_name");
            if (tag != j2.end()) {
                bedrockTagName = tag->second;
            }
        }

        // HACK! This just gets known broken version. Delete when done.
        // bedrockTagName = "2022-05-06";

        // If we've already built this, don't bother doing it again. This makes running this test multiple times in a
        // row much faster.
        prodBedrockName = "/tmp/bedrock-" + bedrockTagName; 
        if (!SFileExists(prodBedrockName)) {
            // Get a directory we can work in.
            char brReleaseDirArr[] = "/tmp/br-prod-test-XXXXXX";
            mkdtemp(brReleaseDirArr);
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

        cout << "Starting prod version at " << prodBedrockName << endl;
        tester = new BedrockClusterTester("db,cache,jobs", prodBedrockName);
    }

    void teardown() {
        delete tester;
    }

    void test() {
        // Verify 0 is leader.
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));

        // Restart 2 on the new version.
        tester->getTester(2).stopServer();
        tester->getTester(2).serverName = "bedrock";
        tester->getTester(2).startServer();
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // Verify 0 and 1 are the same version, but 2 is a different version.
        SData status("Status");
        vector<string> versions(3);
        for (auto i: {0, 1, 2}) {
            vector<SData> statusResult = tester->getTester(i).executeWaitMultipleData({status});
            versions[i] = SParseJSONObject(statusResult[0].content)["version"];
        }
        ASSERT_EQUAL(versions[0], versions[1]);
        ASSERT_NOT_EQUAL(versions[0], versions[2]);

        // Send a write command on 2 and verify we get a response.
        SData cmd("ineffectiveUpdate");
        vector<SData> cmdResult = tester->getTester(2).executeWaitMultipleData({cmd});
        ASSERT_EQUAL(cmdResult[0].methodLine, "200 OK");
    }

} __UpgradeTest;
