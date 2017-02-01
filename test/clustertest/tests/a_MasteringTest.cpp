#include "../BedrockClusterTester.h"

struct a_MasteringTest : tpunit::TestFixture {
    a_MasteringTest()
        : tpunit::TestFixture("a_Mastering",
                              TEST(a_MasteringTest::clusterUp)//,
                              //TEST(a_MasteringTest::failover),
                              //TEST(a_MasteringTest::restoreMaster)
                             ) { }

    BedrockClusterTester* tester;

    void clusterUp()
    {
        // Get the global tester object.
        tester = BedrockClusterTester::testers.front();

        vector<string> results(3);

        // Get the status from each node.
        bool success = false;
        int count = 0;
        while (count++ < 50) {
            for (int i : {0, 1, 2}) {
                BedrockTester* brtester = tester->getBedrockTester(i);

                SData cmd("Status");
                string response = brtester->executeWait(cmd);
                STable json = SParseJSONObject(response);
                results[i] = json["state"];
            }

            if (results[0] == "MASTERING" &&
                results[1] == "SLAVING" &&
                results[2] == "SLAVING")
            {
                success = true;
                break;
            }
            sleep(1);
        }
        ASSERT_TRUE(success);
    }

    void failover()
    {
        tester->stopNode(0);
        BedrockTester* newMaster = tester->getBedrockTester(1);

        int count = 0;
        bool success = false;
        while (count++ < 50) {
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
        while (count++ < 50) {
            SData cmd("Status");
            string response = newMaster->executeWait(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {

                // Ok, let's check the slaves.
                string response1 = tester->getBedrockTester(1)->executeWait(cmd);
                STable json1 = SParseJSONObject(response1);
                if (json1["state"] == "SLAVING") {
                    string response2 = tester->getBedrockTester(2)->executeWait(cmd);
                    STable json2 = SParseJSONObject(response2);
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

} __a_MasteringTest;
