#include "../BedrockClusterTester.h"

struct MultipleMasterSyncTest : tpunit::TestFixture {
    MultipleMasterSyncTest()
        : tpunit::TestFixture("MultipleMasterSyncTest",
                              BEFORE_CLASS(MultipleMasterSyncTest::setup),
                              AFTER_CLASS(MultipleMasterSyncTest::teardown),
                              TEST(MultipleMasterSyncTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void setup() {
        // create a 5 node cluster
        tester = new BedrockClusterTester(BedrockClusterTester::FIVE_NODE_CLUSTER, {"CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)"}, _threadID);

        // make sure the whole cluster is up
        ASSERT_TRUE(tester->getBedrockTester(0)->waitForState("MASTERING"));
        for (int i = 1; i <= 4; i++) {
            ASSERT_TRUE(tester->getBedrockTester(i)->waitForState("SLAVING"));
        }

        // shut down primary master
        tester->stopNode(0);

        // Wait for node 1 to be master.
        ASSERT_TRUE(tester->getBedrockTester(1)->waitForState("MASTERING"));

        // write a bunch more commands
        runTrivialWrites(250, 4);

        // give a beat to make sure everything's in sync
        sleep(10);

        int commitCount;
        std::string debugMsg;
        commitCount = tester->getBedrockTester(4)->getCommitCount();
        debugMsg = "debug: commitCount " + SToStr(commitCount);
        cout << debugMsg << endl;
        ASSERT_TRUE(tester->getBedrockTester(4)->getCommitCount() >= 250);

        // shut down secondary master
        tester->stopNode(1);

        // Wait for node 2 to be master.
        ASSERT_TRUE(tester->getBedrockTester(2)->waitForState("MASTERING"));

        // write a bunch more commands
        runTrivialWrites(250, 4);

        sleep(10);

        // check for the correct number of commits
        commitCount = tester->getBedrockTester(4)->getCommitCount();
        debugMsg = "debug: commitCount " + SToStr(commitCount);
        cout << debugMsg << endl;
        ASSERT_TRUE(tester->getBedrockTester(4)->getCommitCount() >= 500);
    }

    void teardown() {
        delete tester;
    }

    void runTrivialWrites(int writeCount, int nodeID = 0) {
        // Create a bunch of trivial write commands.
        vector<SData> requests(writeCount);
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

        // Send these all to the commandNode.
        BedrockTester* commandNode = tester->getBedrockTester(nodeID);
        commandNode->executeWaitMultipleData(requests);
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
                            // Got a disconnection. Try on the next node.
                            break;
                        }
                    }
                    currentNodeIndex++;
                    currentNodeIndex %= 3;
                }
            });
        }
    }

    void test() {
        // just a check for ready state
        ASSERT_TRUE(tester->getBedrockTester(2)->waitForState("MASTERING"));

        //// dbs are prepped, Let's start spamming.
        //list<thread>* threads = new list<thread>();
        //atomic<bool> done;
        //done.store(false);
        //map<string, int>* counts = new map<string, int>();

        //// start some clients
        //atomic<int> commandID(10000);
        //mutex mu;
        //vector<list<SData>>* allresults = new vector<list<SData>>(60);
        //startClientThreads(*threads, done, *counts, commandID, mu, *allresults);

        // Bring masters back up in reverse order, should go quickly to SYNCHRONIZING
        tester->getBedrockTester(1)->startServer(true);
        tester->getBedrockTester(0)->startServer(true);
        ASSERT_TRUE(tester->getBedrockTester(1)->waitForState("SYNCHRONIZING", 5'000'000 ));
        ASSERT_TRUE(tester->getBedrockTester(0)->waitForState("SYNCHRONIZING", 5'000'000 ));

        // tertiary master should still be MASTERING for a while
        ASSERT_TRUE(tester->getBedrockTester(2)->waitForState("MASTERING", 5'000'000 ));

        // secondary master should catch up first and go MASTERING, wait up to 30s
        ASSERT_TRUE(tester->getBedrockTester(1)->waitForState("MASTERING", 30'000'000 ));

        // when primary master catches up it should go MASTERING, wait up to 30s
        ASSERT_TRUE(tester->getBedrockTester(0)->waitForState("MASTERING", 30'000'000 ));

        //// We're done, let spammers finish.
        //done.store(true);
        //for (auto& t : *threads) {
        //    t.join();
        //}
        //threads->clear();
        //counts->clear();
        //allresults->clear();
        //allresults->resize(60);
        //done.store(false);

        //// Verify everything was either a 202 or a 756.
        //for (auto& p : *counts) {
        //    ASSERT_TRUE(p.first == "202" || p.first == "756");
        //    cout << "method: " << p.first << ", count: " << p.second << endl;
        //}
        
        //// Now that we've verified that, we can start spamming again, and verify failover works in a crash situation.
        //startClientThreads(*threads, done, *counts, commandID, mu, *allresults);

        //// Wait for them to be busy.
        //sleep(2);

        // We're really done, let everything finish a last time.
        //done.store(true);
        //for (auto& t : *threads) {
        //    t.join();
        //}
        //threads->clear();
        //counts->clear();
        //allresults->clear();
        //allresults->resize(60);
        //done.store(false);

        //delete counts;
        //delete threads;
        //delete allresults;
    }

} __MultipleMasterSyncTest;
