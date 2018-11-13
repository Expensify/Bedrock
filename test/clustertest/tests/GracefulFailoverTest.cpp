#include "../BedrockClusterTester.h"

struct GracefulFailoverTest : tpunit::TestFixture {
    GracefulFailoverTest()
        : tpunit::TestFixture("GracefulFailover",
                              BEFORE_CLASS(GracefulFailoverTest::setup),
                              AFTER_CLASS(GracefulFailoverTest::teardown),
                              TEST(GracefulFailoverTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown() {
        delete tester;
    }

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
                        int randNum = SRandom::rand64();
                        int randNum2 = SRandom::rand64();
                        if (randNum % 10 == 0) {
                            if (randNum2 % 5 == 0) {
                                SData query("sendrequest" + randCommand);
                                if (randNum2 % 15 == 0) {
                                    // In this case, let's make them `Connection: forget` to make sure they're
                                    // forgotten.
                                    query["Connection"] = "forget";
                                }
                                query["writeConsistency"] = "ASYNC";
                                query["senttonode"] = to_string(currentNodeIndex);
                                query["clientID"] = to_string(i);
                                query["response"] = "756";
                                requests.push_back(query);
                            }
                        } else if (randNum % 2 == 0) {
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
                            if (randNum2 % 50 == 15) {
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

    bool waitFor(bool start, int nodeNumber, string state, bool logToConsole = false) {
        BedrockTester* node = tester->getBedrockTester(nodeNumber);
        if (start) {
            tester->startNode(nodeNumber, logToConsole);
        }
        int count = 0;
        int success = false;
        while (count++ < 50) {
            SData cmd("Status");
            try {
                string response = node->executeWaitVerifyContent(cmd);
                STable json = SParseJSONObject(response);
                if (json["state"] == state) {
                    success = true;
                    break;
                }
            } catch (const SException& e) {
                // Just try again.
            }

            // Give it another second...
            sleep(1);
        }
        return success;
    }

    string getProp(int nodeNumber, string propName) {
        BedrockTester* node = tester->getBedrockTester(nodeNumber);
        int count = 0;
        while (count++ < 50) {
            try {
                SData cmd("Status");
                string response = node->executeWaitVerifyContent(cmd);
                STable json = SParseJSONObject(response);
                return json[propName];
            } catch (const SException& e) {
                // Just try again.
            }
            // Give it another second...
            sleep(1);
        }
        return "";
    }

    void test() {
        ASSERT_TRUE(waitFor(false, 0, "MASTERING"));

        // Step 1: everything is already up and running. Let's start spamming.
        list<thread>* threads = new list<thread>();
        atomic<bool> done;
        done.store(false);
        map<string, int>* counts = new map<string, int>();

        atomic<int> commandID(10000);
        mutex mu;
        vector<list<SData>>* allresults = new vector<list<SData>>(60);
        startClientThreads(*threads, done, *counts, commandID, mu, *allresults);

        // Let the clients get some activity going, we want everything to be busy.
        sleep(2);

        // Now our clients are spamming all our nodes. Shut down master.
        tester->stopNode(0);

        // Wait for node 1 to be master.
        ASSERT_TRUE(waitFor(false, 1, "MASTERING"));

        // Let the spammers keep spamming on the new master.
        sleep(3);

        // Bring master back up.
        ASSERT_TRUE(waitFor(true, 0, "MASTERING"));
        sleep(15);

        // Now let's  stop a slave and make sure everything keeps working.
        tester->stopNode(2);

        // Wait for master to think the slave is down.
        int count = 0;
        bool success = false;
        while (count++ < 50) {
            string peerList = getProp(0, "peerList");
            list<string> peers = SParseJSONArray(peerList);
            for (auto& peer : peers) {
                STable peerInfo = SParseJSONObject(peer);
                if (peerInfo["name"] == "brcluster_node_2" && peerInfo["State"] == "") {
                    // It's off. We can start it back up.
                    success = true;
                    break;
                } else if (peerInfo["name"] == "brcluster_node_2") {
                    cout << "brcluster_node_2 state is still '" << peerInfo["State"] << "'." << endl;
                }
            }
            sleep(1);
        }
        ASSERT_TRUE(success);

        // And bring it back up.
        ASSERT_TRUE(waitFor(true, 2, "SLAVING"));

        // We're done, let spammers finish.
        done.store(true);
        for (auto& t : *threads) {
            t.join();
        }
        threads->clear();
        counts->clear();
        allresults->clear();
        allresults->resize(60);
        done.store(false);

        // Verify everything was either a 202 or a 756.
        for (auto& p : *counts) {
            ASSERT_TRUE(p.first == "202" || p.first == "756");
            cout << "method: " << p.first << ", count: " << p.second << endl;
        }
        
        // Now that we've verified that, we can start spamming again, and verify failover works in a crash situation.
        startClientThreads(*threads, done, *counts, commandID, mu, *allresults);

        // Wait for them to be busy.
        sleep(2);

        // Blow up master.
        tester->getBedrockTester(0)->stopServer(SIGKILL);

        // Wait for node 1 to be master.
        ASSERT_TRUE(waitFor(false, 1, "MASTERING"));

        // Now bring master back up.
        sleep(2);
        ASSERT_TRUE(waitFor(true, 0, "MASTERING"));

        // Blow up a slave.
        sleep(2);
        tester->getBedrockTester(2)->stopServer(SIGKILL);

        // And bring it back up.
        sleep(2);
        ASSERT_TRUE(waitFor(true, 2, "SLAVING"));

        // We're really done, let everything finish a last time.
        done.store(true);
        for (auto& t : *threads) {
            t.join();
        }
        threads->clear();
        counts->clear();
        allresults->clear();
        allresults->resize(60);
        done.store(false);

        delete counts;
        delete threads;
        delete allresults;
    }

} __GracefulFailoverTest;
