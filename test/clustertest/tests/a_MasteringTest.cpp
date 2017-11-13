#include "../BedrockClusterTester.h"

struct a_MasteringTest : tpunit::TestFixture {
    a_MasteringTest()
        : tpunit::TestFixture("a_Mastering",
                              TEST(a_MasteringTest::clusterUp),
                              TEST(a_MasteringTest::failover),
                              TEST(a_MasteringTest::restoreMaster)
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
                string response = brtester->executeWaitVerifyContent(cmd);
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
            string response = newMaster->executeWaitVerifyContent(cmd);
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

        mutex m;
        int count = 0;
        while (count++ < 10) {
            list<thread> threads;
            vector<string> responses(3);
            for (int i : {0, 1, 2}) {
                threads.emplace_back([this, i, &responses, &m](){
                    BedrockTester* brtester = tester->getBedrockTester(i);

                    SData status("Status");
                    status["writeConsistency"] = "ASYNC";

                    auto result = brtester->executeWaitVerifyContent(status);
                    lock_guard<decltype(m)> lock(m);
                    responses[i] = result;
                });
            }

            // Done.
            for (thread& t : threads) {
                t.join();
            }
            threads.clear();

            STable json0 = SParseJSONObject(responses[0]);
            STable json1 = SParseJSONObject(responses[1]);
            STable json2 = SParseJSONObject(responses[2]);

            if (json0["state"] == "MASTERING" && 
                json1["state"] == "SLAVING" && 
                json2["state"] == "SLAVING") {

                break;
            }
            sleep(1);
        }

        ASSERT_TRUE(count <= 10);
    }

} __a_MasteringTest;
