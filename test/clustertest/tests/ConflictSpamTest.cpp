#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ConflictSpamTest : tpunit::TestFixture
{
    ConflictSpamTest()
        : tpunit::TestFixture("ConflictSpam",
                              BEFORE_CLASS(ConflictSpamTest::setup),
                              AFTER_CLASS(ConflictSpamTest::teardown),
                              TEST(ConflictSpamTest::slow),
                              TEST(ConflictSpamTest::spam),
                              TEST(ConflictSpamTest::blankCommitsReplicateAndSynchronize))
    {
    }

    /* What's a conflict spam test? The main point of this test is to make sure we have lots of conflicting commits
     * coming in to the whole cluster, so that we can make sure they all eventually get committed and replicated in a
     * sane way. This is supposed to be a "worst case scenario" test where we can verify that the database isn't
     * corrupted or anything else horrible happens even in less-than-ideal circumstances.
     */

    BedrockClusterTester* tester;
    atomic<int> cmdID;

    void setup()
    {
        cmdID.store(0);
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    void slow()
    {
        // Send some write commands to each node in the cluster.
        for (int h = 0; h <= 4; h++) {
            for (int i : {0, 1, 2}) {
                BedrockTester& brtester = tester->getTester(i);
                SData query("idcollision b");
                // What if we throw in a few sync commands?
                int cmdNum = cmdID.fetch_add(1);
                query["value"] = "sent-" + to_string(cmdNum);

                // Ok, send.
                brtester.executeWaitVerifyContent(query);
            }
        }

        // Now see if they all match. If they don't, give them a few seconds to sync.
        int tries = 0;
        bool success = false;
        while (tries < 10) {
            vector<string> results(3);
            for (int i : {0, 1, 2}) {
                BedrockTester& brtester = tester->getTester(i);
                SData query("Query");
                query["query"] = "SELECT id, value FROM test ORDER BY id;";
                string result = brtester.executeWaitVerifyContent(query);
                results[i] = result;
            }

            if (results[0] == results[1] && results[1] == results[2] && results[0].size()) {
                success = true;
                break;
            }
            sleep(1);
        }

        ASSERT_TRUE(success);
    }

    void spam()
    {
        recursive_mutex m;
        atomic<int> totalRequestFailures(0);

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &totalRequestFailures](){
                BedrockTester& brtester = tester->getTester(i);

                // Let's make ourselves 20 commands to spam at each node.
                vector<SData> requests;
                int numCommands = 200;
                for (int j = 0; j < numCommands; j++) {
                    SData query("idcollision b2");
                    int cmdNum = cmdID.fetch_add(1);
                    query["value"] = "sent-" + to_string(cmdNum);
                    requests.push_back(query);
                }

                // Ok, send them all!
                auto results = brtester.executeWaitMultipleData(requests);

                int failures = 0;
                for (auto row : results) {
                    if (SToInt(row.methodLine) != 200) {
                        cout << "[ConflictSpamTest] Node " << i << " Expected 200, got: " << SToInt(row.methodLine) << endl;
                        cout << "[ConflictSpamTest] " << row.content << endl;
                        failures++;
                    }
                }
                totalRequestFailures.fetch_add(failures);
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        // Let's collect the names of the journal tables on each node.
        vector<string> allResults(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &allResults, &m](){
                BedrockTester& brtester = tester->getTester(i);

                SData query("Query");
                query["query"] = "SELECT name FROM sqlite_master WHERE type='table';";

                // Ok, send them all!
                auto result = brtester.executeWaitVerifyContent(query);

                SAUTOLOCK(m);
                allResults[i] = result;
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        // Build a list of journal tables on each node.
        vector<list<string>> tables(3);
        int i = 0;
        for (auto result : allResults) {
            list<string> lines = SParseList(result, '\n');
            list<string> output;
            for (auto line : lines) {
                if (SStartsWith(line, "journal") || line == "hct_journal") {
                    output.push_back(line);
                }
            }

            tables[i] = output;
            i++;
        }

        // We'll let this go a couple of times. It's feasible that these won't match if the whole journal hasn't
        // replicated yet.
        int tries = 0;
        while (tries++ < 60) {
            // Wait for both legacy and HC-Tree commits to reach all three nodes.
            allResults.clear();
            allResults.resize(3);
            for (int i : {0, 1, 2}) {
                threads.emplace_back([this, i, &allResults, &m](){
                    BedrockTester& brtester = tester->getTester(i);

                    SData cmd("Query");
                    cmd["query"] = "SELECT MAX(id) FROM journalEntries;";
                    // Ok, send them all!
                    auto result = brtester.executeWaitVerifyContent(cmd);

                    SAUTOLOCK(m);
                    allResults[i] = result;
                });
            }

            // Done.
            for (thread& t : threads) {
                t.join();
            }
            threads.clear();

            if (allResults[0] == allResults[1] && allResults[1] == allResults[2]) {
                break;
            }
            cout << "[ConflictSpamTest] Results didn't match, waiting for journals to equalize." << endl;
            sleep(1);
        }

        // Verify the journals all match.
        ASSERT_TRUE(allResults[0].size() > 0);
        ASSERT_EQUAL(allResults[0], allResults[1]);
        ASSERT_EQUAL(allResults[1], allResults[2]);

        // Let's query the leader DB's journals, and see how many rows each had.
        {
            BedrockTester& brtester = tester->getTester(0);

            auto journals = tables[0];
            vector<SData> commands;
            for (auto journal : journals) {
                string query = "SELECT COUNT(" + string(journal == "hct_journal" ? "cid" : "id") + ") FROM " + journal + ";";

                SData cmd("Query");
                cmd["query"] = query;
                commands.push_back(cmd);
            }

            // Ok, send them all!
            auto results = brtester.executeWaitMultipleData(commands);

            for (size_t i = 0; i < results.size(); i++) {
                // Make sure they all succeeded.
                ASSERT_TRUE(SToInt(results[i].methodLine) == 200);
                list<string> lines = SParseList(results[i].content, '\n');
                lines.pop_front();
            }
            // We can't verify the size of the journal, because we can insert any number of 'upgrade database' rows as
            // each node comes online as leader during startup.
            // ASSERT_EQUAL(totalRows, 69);
        }

        // Spit out the actual table contents, for debugging.
        allResults.clear();
        allResults.resize(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &allResults, &m](){
                BedrockTester& brtester = tester->getTester(i);

                SData cmd("Query");
                cmd["query"] = "SELECT * FROM test;";

                // Ok, send them all!
                auto result = brtester.executeWaitVerifyContent(cmd);

                SAUTOLOCK(m);
                allResults[i] = result;
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        // Verify the actual table contains the right number of rows.
        allResults.clear();
        allResults.resize(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &allResults, &m](){
                BedrockTester& brtester = tester->getTester(i);

                SData cmd("Query");
                cmd["query"] = "SELECT COUNT(id) FROM test;";

                // Ok, send them all!
                auto result = brtester.executeWaitVerifyContent(cmd);

                SAUTOLOCK(m);
                allResults[i] = result;
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        // Verify these came out the same.
        ASSERT_TRUE(allResults[0].size() > 0);
        ASSERT_EQUAL(allResults[0], allResults[1]);
        ASSERT_EQUAL(allResults[1], allResults[2]);

        // And that they're all 66.
        list<string> resultCount = SParseList(allResults[0], '\n');
        resultCount.pop_front();
        ASSERT_EQUAL(cmdID.load(), SToInt(resultCount.front()));

        int fail = totalRequestFailures.load();
        if (fail > 0) {
            cout << "[ConflictSpamTest] Total failures: " << fail << endl;
        }
        ASSERT_EQUAL(fail, 0);
    }

    STable journalState(BedrockTester& node)
    {
        return SParseJSONObject(node.executeWaitVerifyContent(SData("getjournalteststate")));
    }

    void verifyPeerCommit(BedrockTester& observer, const string& peerName, const STable& expected)
    {
        const STable status = SParseJSONObject(observer.executeWaitVerifyContent(SData("Status"), "200", true));
        for (const string& entry : SParseJSONArray(status.at("peerList"))) {
            const STable peer = SParseJSONObject(entry);
            if (peer.at("name") == peerName) {
                EXPECT_EQUAL(peer.at("commitCount"), expected.at("commitCount"));
                EXPECT_EQUAL(peer.at("hashCommitID"), expected.at("hashCommitID"));
                EXPECT_EQUAL(peer.at("hash"), expected.at("hash"));
                return;
            }
        }
        FAIL();
    }

    void verifyBlankCommitData(BedrockTester& node, const vector<int>& expectedValues)
    {
        SQResult data;
        ASSERT_TRUE(node.readDB("SELECT id, value FROM blankCommitTest ORDER BY id;", data));
        ASSERT_EQUAL(data.size(), expectedValues.size());
        for (size_t i = 0; i < data.size(); ++i) {
            EXPECT_EQUAL(data[i]["id"], to_string(i + 1));
            EXPECT_EQUAL(data[i]["value"], to_string(expectedValues[i]));
        }
    }

    void blankCommitsReplicateAndSynchronize()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }

        BedrockTester& leader = tester->getTester(0);
        BedrockTester& liveFollower = tester->getTester(1);
        BedrockTester& syncFollower = tester->getTester(2);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        ASSERT_TRUE(liveFollower.waitForState("FOLLOWING"));
        ASSERT_TRUE(syncFollower.waitForState("FOLLOWING"));

        SData setup("Query");
        setup["Query"] = "CREATE TABLE blankCommitTest(id INTEGER PRIMARY KEY, value INTEGER NOT NULL);";
        leader.executeWaitVerifyContent(setup);
        setup["Query"] = "INSERT INTO blankCommitTest VALUES(1, 0), (2, 0), (3, 0);";
        leader.executeWaitVerifyContent(setup);
        const string startCID = journalState(leader).at("commitCount");
        ASSERT_TRUE(liveFollower.waitForStatusTerm("commitCount", startCID));
        ASSERT_TRUE(syncFollower.waitForStatusTerm("commitCount", startCID));
        tester->stopNode(2);
        const STable liveBefore = journalState(liveFollower);

        // Each group produces one real commit and two blanks. 102 CIDs cross the 101-entry synchronization
        // batch boundary, leaving a final batch containing only a blank, starting immediately after another blank.
        const uint64_t groups = 34;
        vector<int> expectedValues(3, 0);
        for (uint64_t group = 0; group < groups; ++group) {
            vector<SData> requests;
            for (int id = 1; id <= 3; ++id) {
                SData command("blankcommitconflict");
                command["id"] = to_string(id);
                command["processTimeout"] = "10000";
                command["timeout"] = "15000";
                requests.push_back(command);
            }
            const auto responses = leader.executeWaitMultipleData(requests, 3);
            ASSERT_EQUAL(responses.size(), 3ul);
            int successes = 0;
            for (size_t i = 0; i < responses.size(); ++i) {
                if (responses[i].methodLine == "200 OK") {
                    ++successes;
                    ++expectedValues[i];
                } else {
                    ASSERT_EQUAL(responses[i].methodLine, "409 Expected read conflict");
                }
            }
            ASSERT_EQUAL(successes, 1);
        }

        const uint64_t lastCID = SToUInt64(startCID) + groups * 3;
        const STable blankState = journalState(leader);
        ASSERT_EQUAL(blankState.at("commitCount"), to_string(lastCID));
        EXPECT_EQUAL(blankState.at("hashCommitID"), to_string(lastCID - 2));
        const string historyQuery = "SELECT id, hex(query) AS query, hash FROM journalEntries WHERE id > " + startCID + " ORDER BY id;";
        SQResult expectedHistory;
        ASSERT_TRUE(leader.readDB(historyQuery, expectedHistory));
        ASSERT_EQUAL(expectedHistory.size(), groups * 3);
        for (size_t i = 0; i < expectedHistory.size(); ++i) {
            EXPECT_EQUAL(expectedHistory[i]["id"], to_string(SToUInt64(startCID) + i + 1));
            if (i % 3 == 0) {
                EXPECT_FALSE(expectedHistory[i]["query"].empty());
                EXPECT_FALSE(expectedHistory[i]["hash"].empty());
            } else {
                EXPECT_TRUE(expectedHistory[i]["query"].empty());
                EXPECT_TRUE(expectedHistory[i]["hash"].empty());
            }
        }
        EXPECT_EQUAL(blankState.at("hash"), expectedHistory[expectedHistory.size() - 3]["hash"]);

        // Keep the leader idle: no later successful commit may be needed to send the trailing blanks.
        ASSERT_TRUE(liveFollower.waitForStatusTerm("commitCount", to_string(lastCID)));
        const STable liveAfter = journalState(liveFollower);
        EXPECT_EQUAL(liveAfter.at("stateChangeCount"), liveBefore.at("stateChangeCount"));
        EXPECT_EQUAL(liveAfter.at("synchronizeCount"), liveBefore.at("synchronizeCount"));
        EXPECT_EQUAL(liveAfter.at("hashCommitID"), blankState.at("hashCommitID"));
        EXPECT_EQUAL(liveAfter.at("hash"), blankState.at("hash"));
        SQResult liveHistory;
        ASSERT_TRUE(liveFollower.readDB(historyQuery, liveHistory));
        EXPECT_EQUAL(liveHistory.serializeToText(), expectedHistory.serializeToText());
        verifyPeerCommit(liveFollower, leader.getArg("-nodeName"), blankState);

        tester->startNode(2);
        ASSERT_TRUE(syncFollower.waitForState("FOLLOWING"));
        ASSERT_TRUE(syncFollower.waitForStatusTerm("commitCount", to_string(lastCID)));
        const STable syncAfter = journalState(syncFollower);
        EXPECT_GREATER_THAN(SToUInt64(syncAfter.at("synchronizeCount")), 0ull);
        EXPECT_EQUAL(syncAfter.at("hashCommitID"), blankState.at("hashCommitID"));
        EXPECT_EQUAL(syncAfter.at("hash"), blankState.at("hash"));
        SQResult syncHistory;
        ASSERT_TRUE(syncFollower.readDB(historyQuery, syncHistory));
        EXPECT_EQUAL(syncHistory.serializeToText(), expectedHistory.serializeToText());
        verifyPeerCommit(syncFollower, leader.getArg("-nodeName"), blankState);

        // Failed writes must have no effect on application data.
        for (BedrockTester* node : {&leader, &liveFollower, &syncFollower}) {
            verifyBlankCommitData(*node, expectedValues);
        }

        // Ordinary commits must resume at the next CID after the trailing blanks.
        SData write("Query");
        write["Query"] = "UPDATE blankCommitTest SET value = value + 10 WHERE id = 1;";
        leader.executeWaitVerifyContent(write);
        expectedValues[0] += 10;
        const STable finalState = journalState(leader);
        ASSERT_EQUAL(finalState.at("commitCount"), to_string(lastCID + 1));
        EXPECT_EQUAL(finalState.at("hashCommitID"), to_string(lastCID + 1));
        EXPECT_FALSE(finalState.at("hash").empty());
        EXPECT_NOT_EQUAL(finalState.at("hash"), blankState.at("hash"));
        for (BedrockTester* node : {&leader, &liveFollower, &syncFollower}) {
            ASSERT_TRUE(node->waitForStatusTerm("commitCount", to_string(lastCID + 1)));
            const STable state = journalState(*node);
            EXPECT_EQUAL(state.at("hashCommitID"), finalState.at("hashCommitID"));
            EXPECT_EQUAL(state.at("hash"), finalState.at("hash"));
            verifyBlankCommitData(*node, expectedValues);
        }
    }
} __ConflictSpamTest;
