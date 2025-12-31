#include <libstuff/SData.h>
#include <libstuff/SRandom.h>
#include <test/clustertest/BedrockClusterTester.h>

struct GracefulFailoverTest : tpunit::TestFixture
{
    GracefulFailoverTest()
        : tpunit::TestFixture("GracefulFailover",
                              BEFORE_CLASS(GracefulFailoverTest::setup),
                              AFTER_CLASS(GracefulFailoverTest::teardown),
                              TEST(GracefulFailoverTest::test)
        )
    {
    }

    BedrockClusterTester* tester;

    list<thread> threads;
    map<string, int> counts;
    vector<list<SData>> allresults;

    void setup()
    {
        tester = new BedrockClusterTester();
        allresults.resize(60);
    }

    void teardown()
    {
        delete tester;
    }

    void startClientThreads(list<thread>& threads, atomic<bool>& done, map<string, int>& counts,
                            atomic<int>& commandID, mutex& mu, vector<list<SData>>& allresults)
    {
        // Ok, start up some clients.
        for (size_t i = 0; i < allresults.size(); i++) {
            // Start a thread.
            BedrockClusterTester* localTester = tester;
            threads.emplace_back([localTester, i, &mu, &done, &counts, &commandID]() {
                int currentNodeIndex = i % 3;
                while (!done.load()) {
                    // Send some read or some write commands.
                    vector<SData> requests;
                    size_t numCommands = 50;
                    for (size_t j = 0; j < numCommands; j++) {
                        string randCommand = " r_" + to_string(commandID.fetch_add(1)) + "_r";
                        // Every 10th client makes HTTPS requests (1/5th as many, cause they take forever).
                        // We ask for `756` responses to verify we don't accidentally get back something besides what
                        // we expect (some default value).
                        auto randNum = SRandom::rand64();
                        auto randNum2 = SRandom::rand64();
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
                    BedrockTester& node = localTester->getTester(currentNodeIndex);
                    auto results = node.executeWaitMultipleData(requests, 1, false, true);
                    for (auto& r : results) {
                        lock_guard<mutex> lock(mu);
                        if (r.methodLine != "002 Socket Failed") {
                            if (counts.find(r.methodLine) != counts.end()) {
                                counts[r.methodLine]++;
                            } else {
                                counts[r.methodLine] = 1;
                            }
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

    void test()
    {
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));

        // Step 1: everything is already up and running. Let's start spamming.
        list<thread> threads;
        map<string, int> counts;
        vector<list<SData>> allresults(60);
        atomic<bool> done;
        done.store(false);

        atomic<int> commandID(10000);
        mutex mu;
        startClientThreads(threads, done, counts, commandID, mu, allresults);

        // Let the clients get some activity going, we want everything to be busy.
        sleep(2);

        // Now our clients are spamming all our nodes. Shut down leader.
        tester->stopNode(0);

        // Wait for node 1 to be leader.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));

        // Let the spammers keep spamming on the new leader.
        sleep(3);

        // Bring leader back up.
        tester->getTester(0).startServer();
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));
        sleep(15);

        // Now let's  stop a follower and make sure everything keeps working.
        tester->stopNode(2);

        // Wait up to 90 seconds for leader to think the follower is down.
        uint64_t start = STimeNow();
        bool success = false;
        while (STimeNow() < start + 90'000'000) {
            string response = tester->getTester(0).executeWaitVerifyContent(SData("Status"));
            STable json = SParseJSONObject(response);
            string peerList = json["peerList"];
            list<string> peers = SParseJSONArray(peerList);
            for (auto& peer : peers) {
                STable peerInfo = SParseJSONObject(peer);
                if (peerInfo["name"] == "cluster_node_2" && (peerInfo["State"] == "" || SStartsWith(peerInfo["State"], "SEARCHING"))) {
                    success = true;
                    break;
                }
            }
            if (success) {
                break;
            }
            usleep(100'000);
        }
        ASSERT_TRUE(success);

        // And bring it back up.
        tester->getTester(2).startServer();
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // We're done, let spammers finish.
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
            cout << "[GracefulFailoverTest] method: " << p.first << ", count: " << p.second << endl;
        }

        // Now that we've verified that, we can start spamming again, and verify failover works in a crash situation.
        startClientThreads(threads, done, counts, commandID, mu, allresults);

        // Wait for them to be busy.
        sleep(2);

        // Blow up leader.
        tester->getTester(0).stopServer(SIGKILL);

        // Wait for node 1 to be leader.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));

        // Now bring leader back up.
        sleep(2);
        tester->getTester(0).startServer();
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));

        // Blow up a follower.
        sleep(2);
        tester->getTester(2).stopServer(SIGKILL);

        // And bring it back up.
        sleep(2);
        tester->getTester(2).startServer();
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // We're really done, let everything finish a last time.
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
} __GracefulFailoverTest;
