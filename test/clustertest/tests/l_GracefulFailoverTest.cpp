#include "../BedrockClusterTester.h"

struct l_GracefulFailoverTest : tpunit::TestFixture {
    l_GracefulFailoverTest()
        : tpunit::TestFixture("l_GracefulFailover",
                              TEST(l_GracefulFailoverTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void startClientThreads(list<thread>& threads, atomic<bool>& done, map<string, int>& counts,
                            atomic<int>& commandID, mutex& mu, vector<list<SData>>& allresults) {
        // Ok, start up some clients.
        for (size_t i = 0; i < allresults.size(); i++) {
            // Start a thread.
            BedrockClusterTester* localTester = tester;
            threads.emplace_back([localTester, i, &mu, &done, &allresults, &counts, &commandID]() {
                int currentNodeIndex = i % 3;
                while(!done.load()) {
                    // Send some read or some write commands.
                    vector<SData> requests;
                    size_t numCommands = 50;
                    for (size_t j = 0; j < numCommands; j++) {
                        string randCommand = " r_" + to_string(commandID.fetch_add(1)) + "_r";
                        // Every 10th client makes HTTPS requests (1/5th as many, cause they take forever).
                        // We ask for `756` responses to verify we don't accidentally get back something besides what
                        // we expect (some default value).
                        if (i % 10 == 0) {
                            if (j % 5 == 0) {
                                SData query("sendrequest" + randCommand);
                                query["writeConsistency"] = "ASYNC";
                                query["senttonode"] = to_string(currentNodeIndex);
                                query["clientID"] = to_string(i);
                                query["response"] = "756";
                                requests.push_back(query);
                            }
                        } else if (i % 2 == 0) {
                            // Every remaining even client makes write requests.
                            SData query("idcollision" + randCommand);
                            query["writeConsistency"] = "ASYNC";
                            query["peekSleep"] = "5";
                            query["processSleep"] = "5";
                            query["response"] = "756";
                            query["senttonode"] = to_string(currentNodeIndex);
                            query["clientID"] = to_string(i);
                            requests.push_back(query);
                        } else {
                            // Any other client makes read requests.
                            SData query("testcommand" + randCommand);
                            // A few of them will get scheduled in the future to make sure they don't block shutdown.
                            if (j % 50 == 15) {
                                query["commandExecuteTime"] = to_string(STimeNow() + 1000000 * 60);
                            }
                            query["peekSleep"] = "10";
                            query["response"] = "756";
                            query["senttonode"] = to_string(currentNodeIndex);
                            query["clientID"] = to_string(i);
                            requests.push_back(query);
                        }
                    }

                    // Ok, send them all!
                    BedrockTester* node = localTester->getBedrockTester(currentNodeIndex);
                    auto results = node->executeWaitMultipleData(requests, 1, false, true);
                    size_t completed = 0;
                    for (auto& r : results) {
                        lock_guard<mutex> lock(mu);
                        if (r.methodLine != "002 Socket Failed") {
                            if (counts.find(r.methodLine) != counts.end()) {
                                counts[r.methodLine]++;
                            } else {
                                counts[r.methodLine] = 1;
                            }
                            completed++;
                        } else {
                            // Got a disconnection. try on the next node.
                            break;
                        }
                    }
                    currentNodeIndex++;
                    currentNodeIndex %= 3;
                }
            });
        }
    }

    void test()
    {
        // Verify the existing master is up.
        tester = BedrockClusterTester::testers.front();
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

        // Step 1: everything is already up and running. Let's start spamming.
        list<thread> threads;
        atomic<bool> done;
        done.store(false);
        map<string, int> counts;

        atomic<int> commandID(10000);
        mutex mu;
        vector<list<SData>> allresults(60);
        startClientThreads(threads, done, counts, commandID, mu, allresults);

        // Let the clients get some activity going, we want everything to be busy.
        sleep(2);

        // Now our clients are spamming all our nodes. Shut down master.
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

        // Let the spammers spam.
        sleep(3);

        // Bring master back up.
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

        // make sure it actually succeeded.
        ASSERT_TRUE(success);

        // We're done, let everything finish.
        done.store(true);
        for (auto& t : threads) {
            t.join();
        }
        threads.clear();
        counts.clear();
        allresults.clear();
        allresults.resize(60);
        done.store(false);

        // Verify everything was either a 202 or a 756.
        for (auto& p : counts) {
            ASSERT_TRUE(p.first == "202" || p.first == "756");
            cout << "method: " << p.first << ", count: " << p.second << endl;
        }
        
        // Now that we've verified that, we can start spamming again, and verify failover works in a crash situation.
        startClientThreads(threads, done, counts, commandID, mu, allresults);

        // Wait for them to be busy.
        sleep(2);

        // Blow up master.
        master = tester->getBedrockTester(0);
        master->stopServer(SIGKILL);

        // Wait for node 1 to be master.
        newMaster = tester->getBedrockTester(1);
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

        // We're done, let everything finish.
        done.store(true);
        for (auto& t : threads) {
            t.join();
        }
        threads.clear();
        counts.clear();
        allresults.clear();
        allresults.resize(60);
        done.store(false);
    }

} __l_GracefulFailoverTest;
