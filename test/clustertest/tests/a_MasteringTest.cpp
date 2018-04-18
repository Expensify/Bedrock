#include "../BedrockClusterTester.h"

struct a_MasteringTest : tpunit::TestFixture {
    a_MasteringTest()
        : tpunit::TestFixture("a_Mastering",
                              BEFORE_CLASS(a_MasteringTest::setup),
                              AFTER_CLASS(a_MasteringTest::teardown),
                              TEST(a_MasteringTest::clusterUp),
                              TEST(a_MasteringTest::failover),
                              // Disabled for speed. Enable to test stand down timeout.
                              // TEST(a_MasteringTest::standDownTimeout),
                              TEST(a_MasteringTest::restoreMaster),
                              TEST(a_MasteringTest::synchronizing)
                             ) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown () {
        delete tester;
    }

    void clusterUp()
    {
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

    // The only point of this test is to verify that a new master comes up even if the old one has a stuck HTTPS
    // request. It's slow so is disabled.
    void standDownTimeout() {
        BedrockTester* newMaster = tester->getBedrockTester(1);
        SData cmd("httpstimeout");
        cmd["Connection"] = "forget";
        auto result = newMaster->executeWaitVerifyContent(cmd, "202");
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

    void synchronizing() {
        // Stop a slave.
        tester->stopNode(1);

        // Create a bunch of commands.
        vector<SData> requests(1000);
        int count = 0;
        for (auto& request : requests) {
            if (!count) {
                request.methodLine = "Query";
                request["writeConsistency"] = "ASYNC";
                request["query"] = "INSERT INTO test VALUES(12345, '');";
                count++;
            } else {
                request.methodLine = "Query";
                request["writeConsistency"] = "ASYNC";
                request["query"] = "UPDATE test SET value = 'xxx" + to_string(count++) + "' WHERE id = 12345;";
            }
        }

        // Send these all to master.
        BedrockTester* master = tester->getBedrockTester(0);
        master->executeWaitMultipleData(requests);

        // Start the slave back up.
        bool wasSynchronizing = false;
        string startstatus = tester->startNodeDontWait(1);
        STable json = SParseJSONObject(startstatus);
        if (json["state"] == "SYNCHRONIZING") {
            wasSynchronizing = true;
        }

        // Verify it goes SYNCHRONIZING and then SLAVING.
        BedrockTester* slave = tester->getBedrockTester(1);
        int tries = 0;
        while (1) {
            SData status("Status");
            auto result = slave->executeWaitVerifyContent(status, "200", true);
            STable json = SParseJSONObject(result);

            if (!wasSynchronizing) {
                if(json["state"] == "SYNCHRONIZING") {
                    wasSynchronizing = true;
                    continue;
                }
            }
            if(json["state"] == "SLAVING") {
                break;
            }
            tries++;
            if (tries > 6000) {
                STHROW("Timed out waiting for synchronizing and then mastering.");
            }
            usleep(10'000); // 1/100th of a second
        }
        ASSERT_TRUE(wasSynchronizing);
    }

} __a_MasteringTest;
