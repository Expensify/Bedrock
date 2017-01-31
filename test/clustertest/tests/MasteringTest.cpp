#include "../BedrockClusterTester.h"

struct MasteringTest : tpunit::TestFixture {
    MasteringTest()
        : tpunit::TestFixture(
                              TEST(MasteringTest::clusterUp),
                              TEST(MasteringTest::failover),
                              TEST(MasteringTest::restoreMaster)
                             ) {
        NAME(Mastering);
    }

    BedrockClusterTester* tester;

    void clusterUp()
    {
        // Get the global tester object.
        tester = BedrockClusterTester::testers.front();

        list<BedrockTester*> brtesters;
        for (auto i : {0, 1, 2}) {
            brtesters.push_back(tester->getBedrockTester(i));
        }

        list<string> results;

        // Get the status from each node.
        for (auto& brtest : brtesters) {
            SData cmd("Status");
            string response = brtest->executeWait(cmd);
            STable json = SParseJSONObject(response);
            results.push_back(json["state"]);
        }

        // Check that the first is mastering and the others are slaving.
        int current = 0;
        for (string result : results) {
            if (current) {
                ASSERT_EQUAL(result, "SLAVING");
            } else {
                ASSERT_EQUAL(result, "MASTERING");
            }
            current++;
        }
    }

    void failover()
    {
        tester->stopNode(0);
        BedrockTester* newMaster = tester->getBedrockTester(1);

        int count = 0;
        bool success = false;
        while (count++ < 10) {
            SData cmd("Status");
            string response = newMaster->executeWait(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }

            // Give it another second...
            sleep(1);
        }

        ASSERT_TRUE(success);
    }

    void restoreMaster()
    {
        tester->startNode(0);
        BedrockTester* newMaster = tester->getBedrockTester(0);

        int count = 0;
        while (count++ < 10) {
            SData cmd("Status");
            string response = newMaster->executeWait(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {

                // Ok, let's check the slaves.
                string response1 = tester->getBedrockTester(1)->executeWait(cmd);
                STable json1 = SParseJSONObject(response);
                if (json1["state"] == "SLAVING") {
                    string response2 = tester->getBedrockTester(2)->executeWait(cmd);
                    STable json2 = SParseJSONObject(response);
                    if (json2["state"] == "SLAVING") {
                        break;
                    }
                }
            }

            // Give it another second...
            sleep(1);
        }

        // Should be back up, let's verify the cluster one last time.
        SData cmd("Status");
        vector<string> responses(3);
        for (int i : {0, 1, 2}) {
            string response = tester->getBedrockTester(i)->executeWait(cmd);
            STable json = SParseJSONObject(response);
            responses[i] = json["state"];
        }

        ASSERT_EQUAL(responses[0], "MASTERING");
        ASSERT_EQUAL(responses[1], "SLAVING");
        ASSERT_EQUAL(responses[2], "SLAVING");
    }

} __MasteringTest;
