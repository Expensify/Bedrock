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
        // Spin up the cluster in one thread.
        thread t1([&]() {
            tester = new BedrockClusterTester();
        });
        
        // Download the latest binary in another.
        thread t2([&]() {
            const string tempJson = "brdata.json";
            string command = "curl --silent 'https://api.github.com/repos/Expensify/Bedrock/releases?page=1&per_page=1' -o " + tempJson;
            system(command.c_str());
            string data = SFileLoad("brdata.json");

            string bedrockDownloadURL;
            list<string> j1 = SParseJSONArray(STrim(data));
            if (j1.size()) {
                STable j2 = SParseJSONObject(j1.front());
                auto assets = j2.find("assets");
                if (assets != j2.end()) {
                    list<string> j3 = SParseJSONArray(assets->second);
                    for (auto& item : j3) {
                        STable j4 = SParseJSONObject(item);
                        auto name = j4.find("name");
                        if (name != j4.end() && name->second == "bedrock") {
                            bedrockDownloadURL = j4["browser_download_url"];
                        }
                    }
                }
            }

            const string releaseBedrock = "release_bedrock";
            cout << "URL: " << bedrockDownloadURL << endl;
            SFileDelete(tempJson);
            ASSERT_TRUE(bedrockDownloadURL.size());
            command = "curl --silent -L '" + bedrockDownloadURL + "' -o " + releaseBedrock;
            system(command.c_str());
            struct stat fileInfo{0};
            stat(releaseBedrock.c_str(), &fileInfo);
            chmod(releaseBedrock.c_str(), fileInfo.st_mode | S_IXUSR);
            cout << "Downloaded production bedrock" << endl;
        });

        // Wait for them both to finish.
        t1.join();
        t2.join();
    }

    void teardown() {
        delete tester;
    }

    void test() {
    }

} __UpgradeTest;
