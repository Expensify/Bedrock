#include "../BedrockClusterTester.h"

struct k_GracefulFailoverTest : tpunit::TestFixture {
    k_GracefulFailoverTest()
        : tpunit::TestFixture("k_GracefulFailover",
                              TEST(k_GracefulFailoverTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void test()
    {
        // So here, we start with node 0 mastering.
        // We then spin up a few threads that continually spam all three nodes with both read and write commands.
        // These should complete locally or escalate to master as appropriate.
        // We then shut down node 0. Node 1 will take over as master.
        // All commands sent to Node 0 after this point should result in a "connection refused" error. This is fine.
        // We verify that node 1 comes up as master.
        // We then continue spamming for a few seconds and make sure every command returns either success, or
        // connection refused.
        // Then we bring Node 0 back up, and verify that it takes over as master. Send a few more commands.
        //
        // We should have sent hundreds of commands, and they all should have either succeeded, or been "connection
        // refused".
        //
        // Thus concludes our test:
        // TODO:
        // https commands.
        // Commands scheduled in the future, or waiting on future commits.
        //

        // Step 1: everything is already up and running. Let's start spamming.
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();
         
        list<thread> threads;
        atomic<bool> done;
        done.store(false);
        vector<list<SData>> allresults(3);
        for (int i = 0; i < 3; i++) {
            // Start a thread.
            threads.emplace_back([tester, i, &done, &allresults]() {
                BedrockTester* node = tester->getBedrockTester(i);
                while(!done.load()) {
                    // Send some read and some write commands.
                    vector<SData> requests;
                    int numCommands = 200;
                    for (int j = 0; j < numCommands; j++) {
                        if (j % 2) {
                            SData query("idcollision");
                            query["writeConsistency"] = "ASYNC";
                            query["peekSleep"] = "100";
                            query["processSleep"] = "100";
                            requests.push_back(query);
                        } else {
                            SData query("testcommand");
                            query["peekSleep"] = "100";
                            requests.push_back(query);
                        }
                    }

                    // Ok, send them all!
                    auto results = node->executeWaitMultipleData(requests, 50);
                    for (auto& r : results) {
                        allresults[i].push_back(r);
                    }
                }
            });
        }

        // Now our clients are spamming all our nodes. Shut down master.
        tester->stopNode(0);

        // Wait for node 1 to be master.
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

        // make sure it actually succeeded.
        ASSERT_TRUE(success);

        // Let the spammers spam.
        sleep (3);

        // Bring master back up.
        tester->startNode(0);
        BedrockTester* master = tester->getBedrockTester(0);

        count = 0;
        success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = master->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }

            // Give it another second...
            sleep(1);
        }

        // make sure it actually succeeded.
        ASSERT_TRUE(success);

        // Great, it came back up.
        done.store(true);

        for (auto& t : threads) {
            t.join();
            // TODO: Verify the results of our spamming.
        }

        for (auto& results : allresults) {
            for (auto &r : results) {
                bool valid = r.methodLine == "200 OK" || r.methodLine == "001 No Socket";
                if (!valid) {
                    cout << r.methodLine << endl;
                }
                ASSERT_TRUE(valid);
            }
        }
    }

} __k_GracefulFailoverTest;
