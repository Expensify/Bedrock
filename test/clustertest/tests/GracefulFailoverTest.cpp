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

    struct ClientLoad
    {
        list<thread> threads;
        atomic<bool> done{false};
        BedrockTester* blockedLeader = nullptr;

        void stop()
        {
            done = true;
            if (blockedLeader) {
                try {
                    blockedLeader->executeWaitMultipleData({SData("UnblockWrites")}, 1, true, true);
                } catch (...) {
                    // Cleanup must still join the clients when the server is already gone.
                }
                blockedLeader = nullptr;
            }
            for (auto& client : threads) {
                if (client.joinable()) {
                    client.join();
                }
            }
            threads.clear();
        }

        ~ClientLoad()
        {
            stop();
        }
    };

    void setup()
    {
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    void startClientThreads(ClientLoad& clients, map<string, int>& counts, atomic<int>& commandID, mutex& mu)
    {
        clients.done = false;
        for (size_t i = 0; i < 60; i++) {
            // Start a thread.
            BedrockClusterTester* localTester = tester;
            clients.threads.emplace_back([localTester, i, &mu, &clients, &counts, &commandID]() {
                int currentNodeIndex = i % 3;
                while (!clients.done.load()) {
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
                                query["senttonode"] = to_string(currentNodeIndex);
                                query["clientID"] = to_string(i);
                                query["response"] = "756";
                                requests.push_back(query);
                            }
                        } else if (randNum % 2 == 0) {
                            // Every remaining even client makes write requests.
                            SData query("idcollision" + randCommand);
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
                        // A deliberate disconnect leaves this and the unattempted requests without a response.
                        if (r.methodLine.empty()) {
                            break;
                        }
                        lock_guard<mutex> lock(mu);
                        ++counts[r.methodLine];
                    }
                    currentNodeIndex++;
                    currentNodeIndex %= 3;
                }
            });
        }
    }

    bool responsesValid(const map<string, int>& counts, bool allowLostEscalations = false)
    {
        bool valid = true;
        int completed = 0;
        for (const auto& [method, count] : counts) {
            const int code = SToInt(method);
            if (code == 756) {
                completed += count;
            }
            // After SIGKILL, the messenger cannot safely replay requests whose responses were lost.
            if (allowLostEscalations && method == "500 Internal Server Error") {
                continue;
            }
            if (code != 202 && code != 756) {
                cout << "[GracefulFailoverTest] Unexpected response: " << method << ", count: " << count << endl;
                valid = false;
            }
        }
        return valid && completed > 0;
    }

    void test()
    {
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));

        // Step 1: everything is already up and running. Let's start spamming.
        map<string, int> counts;
        atomic<int> commandID(10000);
        mutex mu;
        ClientLoad clients;
        startClientThreads(clients, counts, commandID, mu);

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
        clients.stop();
        ASSERT_TRUE(responsesValid(counts));
        counts.clear();

        // Now that we've verified that, we can start spamming again, and verify failover works in a crash situation.
        startClientThreads(clients, counts, commandID, mu);

        // Wait for them to be busy.
        sleep(2);

        // An async leader can fork if killed with unreplicated commits. Establish a common checkpoint before
        // testing crash recovery, while clients continue issuing requests against the blocked leader.
        BedrockTester& leader = tester->getTester(0);
        clients.blockedLeader = &leader;
        leader.executeWaitVerifyContent(SData("BlockWrites"), "200 Blocked", true);
        const string checkpointID = SParseJSONObject(leader.executeWaitVerifyContent(SData("Status"), "200", true))["commitCount"];
        SData getCheckpoint("GetCommitHash");
        getCheckpoint["commitCount"] = checkpointID;
        const SData checkpoint = leader.executeWaitMultipleData({getCheckpoint}, 1, true).front();
        ASSERT_EQUAL(checkpoint.methodLine, "200 OK");
        for (size_t i : {1, 2}) {
            BedrockTester& follower = tester->getTester(i);
            ASSERT_TRUE(follower.waitForStatusTerm("commitCount", checkpointID));
            const SData replicated = follower.executeWaitMultipleData({getCheckpoint}, 1, true).front();
            ASSERT_EQUAL(replicated.methodLine, "200 OK");
            ASSERT_EQUAL(replicated["hash"], checkpoint["hash"]);
        }

        // Blow up leader without releasing the write block, so no newer unreplicated commit can slip in.
        tester->getTester(0).stopServer(SIGKILL);
        clients.blockedLeader = nullptr;

        // Wait for node 1 to be leader.
        ASSERT_TRUE(tester->getTester(1).waitForState("LEADING"));

        // Now bring leader back up.
        sleep(2);
        tester->getTester(0).startServer();
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));
        const SData recovered = leader.executeWaitMultipleData({getCheckpoint}, 1, true).front();
        ASSERT_EQUAL(recovered.methodLine, "200 OK");
        ASSERT_EQUAL(recovered["hash"], checkpoint["hash"]);

        // Blow up a follower.
        sleep(2);
        tester->getTester(2).stopServer(SIGKILL);

        // And bring it back up.
        sleep(2);
        tester->getTester(2).startServer();
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        // We're really done, let everything finish a last time.
        clients.stop();
        ASSERT_TRUE(responsesValid(counts, true));
    }
} __GracefulFailoverTest;
