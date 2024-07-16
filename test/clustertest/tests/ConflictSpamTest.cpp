#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ConflictSpamTest : tpunit::TestFixture {
    ConflictSpamTest() : tpunit::TestFixture("ConflictSpam") {
        registerTests(BEFORE_CLASS(ConflictSpamTest::setup),
                      AFTER_CLASS(ConflictSpamTest::teardown),
                      TEST(ConflictSpamTest::slow),
                      TEST(ConflictSpamTest::spam));
    }

    /* What's a conflict spam test? The main point of this test is to make sure we have lots of conflicting commits
     * coming in to the whole cluster, so that we can make sure they all eventually get committed and replicated in a
     * sane way. This is supposed to be a "worst case scenario" test where we can verify that the database isn't
     * corrupted or anything else horrible happens even in less-than-ideal circumstances.
     */

    BedrockClusterTester* tester;
    atomic<int> cmdID;

    void setup() {
        cmdID.store(0);
        tester = new BedrockClusterTester();
    }

    void teardown() {
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
                query["writeConsistency"] = "ASYNC";
                int cmdNum = cmdID.fetch_add(1);
                query["value"] = "sent-" + to_string(cmdNum);

                // Ok, send.
                string result = brtester.executeWaitVerifyContent(query);
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
                query["writeConsistency"] = "ASYNC";
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
            threads.emplace_back([this, i, &totalRequestFailures, &m](){
                BedrockTester& brtester = tester->getTester(i);

                // Let's make ourselves 20 commands to spam at each node.
                vector<SData> requests;
                int numCommands = 200;
                for (int j = 0; j < numCommands; j++) {
                    SData query("idcollision b2");
                    query["writeConsistency"] = "ASYNC";
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
        vector <string> allResults(3);
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
                if (SStartsWith(line, "journal")) {
                    output.push_back(line);
                }
            }

            tables[i] = output;
            i++;
        }

        // We'll let this go a couple of times. It's feasible that these won't match if the whole journal hasn't
        // replicated yet.
        int tries = 0;
        while(tries++ < 60) {

            // Now lets compose a query for the journal of each node.
            allResults.clear();
            allResults.resize(3);
            for (int i : {0, 1, 2}) {
                threads.emplace_back([this, i, &allResults, &tables, &m](){
                    BedrockTester& brtester = tester->getTester(i);

                    auto journals = tables[i];
                    list <string> queries;
                    for (auto journal : journals) {
                        queries.push_back("SELECT MAX(id) as maxIDs FROM " + journal);
                    }

                    string query = "SELECT MAX(maxIDs) FROM (" + SComposeList(queries, " UNION ");
                    query += ");";

                    SData cmd("Query");
                    cmd["query"] = query;
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
            vector <SData> commands;
            for (auto journal : journals) {
                string query = "SELECT COUNT(id) FROM " + journal + ";";

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
            threads.emplace_back([this, i, &allResults, &tables, &m](){
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
            threads.emplace_back([this, i, &allResults, &tables, &m](){
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

} __ConflictSpamTest;
