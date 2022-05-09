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

    void setup() {
        // Get a directory we can work in.
        char brReleaseDirArr[] = "/tmp/br-prod-test-XXXXXX";
        mkdtemp(brReleaseDirArr);
        string brReleaseDir(brReleaseDirArr, sizeof(brReleaseDirArr) - 1);
        cout << "Using temp dir for prod bedrock: " << brReleaseDir << endl;

        // Get the list of recent releases.
        const string tempJson = "brdata.json";
        string command = "curl --silent 'https://api.github.com/repos/Expensify/Bedrock/releases?page=1&per_page=1' -o " + brReleaseDir + "/" + tempJson;
        cout << command << endl;
        if (int result = system(command.c_str())) {
            cout << "Couldn't download bedrock releases: " << result << ":" << errno << endl;
        }

        // Parse a tag from it.
        string data = SFileLoad(brReleaseDir + "/" + tempJson);
        string bedrockTagName;
        list<string> j1 = SParseJSONArray(STrim(data));
        if (j1.size()) {
            STable j2 = SParseJSONObject(j1.front());
            auto tag = j2.find("tag_name");
            if (tag != j2.end()) {
                bedrockTagName = tag->second;
            }
        }

        // Clone bedrock
        if (system(("cd " + brReleaseDir + " && git clone https://github.com/Expensify/Bedrock.git > /dev/null").c_str())) {
            cout << "Couldn't clone Bedrock" << endl;
            return;
        }

        // Check out the release tag.
        if (system(("cd " + brReleaseDir + " && cd Bedrock && git checkout " + bedrockTagName + "  > /dev/null").c_str())) {
            cout << "Couldn't heckout latest release" << endl;
            return;
        }

        // Build the release.
        if (system(("cd " + brReleaseDir + " && cd Bedrock && make -j8 bedrock  > /dev/null").c_str())) {
            cout << "Couldn't build release bedrock" << endl;
            return;
        }

        tester = new BedrockClusterTester("db,cache,jobs", brReleaseDir + "/Bedrock/bedrock");
        cout << "Starting prod bedrock cluster" << endl;
        sleep(10);
    }

    void teardown() {
        delete tester;
    }

    void test() {
    }

} __UpgradeTest;
