#include "../BedrockClusterTester.h"

struct l_GracefulFailoverTest : tpunit::TestFixture {
    l_GracefulFailoverTest()
        : tpunit::TestFixture("l_GracefulFailover",
                              TEST(l_GracefulFailoverTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void test()
    {
        // Verify the existing master is up.
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();
        BedrockTester* master = tester->getBedrockTester(0);

        int count = 0;
        bool success = false;
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

        cout << "======================================== node 0 is mastering." << endl;

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
        list<thread> threads;
        atomic<bool> done;
        done.store(false);
        mutex m;
        vector<list<SData>> allresults(50);

        // Ok, start up 50 clients.
        for (int i = 0; i < 50; i++) {
            // Start a thread.
            threads.emplace_back([tester, i, &m, &done, &allresults]() {
                int currentNodeIndex = i % 3;
                while(!done.load()) {
                    // Send some read or some write commands.
                    vector<SData> requests;
                    size_t numCommands = 50;
                    for (size_t j = 0; j < numCommands; j++) {
                        if (i % 2) {
                            SData query("idcollision");
                            query["writeConsistency"] = "ASYNC";
                            query["peekSleep"] = "20";
                            query["processSleep"] = "20";
                            query["response"] = "756";
                            requests.push_back(query);
                        } else {
                            SData query("testcommand");
                            query["peekSleep"] = "20";
                            query["response"] = "756";
                            requests.push_back(query);
                        }
                    }

                    // Ok, send them all!
                    cout << "Client " << i << " sending " << numCommands << " to node " << currentNodeIndex << endl;
                    BedrockTester* node = tester->getBedrockTester(currentNodeIndex);
                    auto results = node->executeWaitMultipleData(requests, 1, false, true);
                    size_t completed = 0;
                    for (auto& r : results) {
                        lock_guard<mutex> lock(m);
                        if (r.methodLine != "002 Socket Failed") {
                            if (r.methodLine != "756") {
                                cout << "Client "<< i << " expected 756, got: '" << r.methodLine <<  "', had completed: " << completed << endl;
                            }
                            allresults[i].push_back(r);
                            completed++;
                        } else {
                            // Got a disconnection. try on the next node.
                            break;
                        }
                    }
                    cout << "Completed " << completed << " commands of "  << numCommands << " on client " << i << " and node " << currentNodeIndex << endl;

                    if (completed != numCommands) {
                        cout << "Disconnected, trying next node." << endl;
                        currentNodeIndex++;
                        currentNodeIndex %= 3;
                    }
                }
            });
        }

        // Let the clients get started.
        sleep(2);

        // Now our clients are spamming all our nodes. Shut down master.
        cout << "======================================== node 0 is stopping." << endl;
        tester->stopNode(0);

        // Wait for node 1 to be master.
        BedrockTester* newMaster = tester->getBedrockTester(1);
        count = 0;
        success = false;
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
        cout << "======================================== node 1 is mastering." << endl;

        // Let the spammers spam.
        sleep(3);

        // Bring master back up.
        cout << "======================================== node 0 is starting." << endl;
        tester->startNode(0);

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

        cout << "======================================== node 0 is mastering." << endl;

        // make sure it actually succeeded.
        ASSERT_TRUE(success);

        // Great, it came back up.
        done.store(true);

        for (auto& t : threads) {
            t.join();
            // TODO: Verify the results of our spamming.
        }

        map<string, int> counts;
        for (auto& results : allresults) {
            for (auto &r : results) {
                if (counts.find(r.methodLine) != counts.end()) {
                    counts[r.methodLine]++;
                } else {
                    counts[r.methodLine] = 1;
                }
            }
        }
        for (auto& p : counts) {
            cout << "method: " << p.first << ", count: " << p.second << endl;
        }
        for (auto& results : allresults) {
            for (auto &r : results) {
                bool valid = r.methodLine == "756";
                if (!valid) {
                    cout << "invalid: " << r.methodLine << endl;
                }
                ASSERT_TRUE(valid);
            }
        }
    }

} __l_GracefulFailoverTest;
