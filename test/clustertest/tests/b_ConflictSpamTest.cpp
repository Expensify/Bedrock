#include "../BedrockClusterTester.h"

struct b_ConflictSpamTest : tpunit::TestFixture {
    b_ConflictSpamTest()
        : tpunit::TestFixture("b_ConflictSpam",
                              TEST(b_ConflictSpamTest::slow),
                              TEST(b_ConflictSpamTest::spam)) { }

    /* What's a conflict spam test? The main point of this test is to make sure we have lots of conflicting commits
     * coming in to the whole cluster, so that we can make sure they all eventually get committed and replicated in a
     * sane way. This is supposed to be a "worst case scenario" test where we can verify that the database isn't
     * corrupted or anything else horrible happens even in less-than-ideal circumstances.
     */

    BedrockClusterTester* tester;

    void slow()
    {
        tester = BedrockClusterTester::testers.front();

        // Send a write command to each node in the cluster, twice.
        for (int h = 0; h <= 4; h++) {
            for (int i : {0, 1, 2}) {
                int cmdID = h * 3 + i;
                BedrockTester* brtester = tester->getBedrockTester(i);
                SData query("Query");
                // What if we throw in a few sync commands?
                if (h != 2) {
                    query["writeConsistency"] = "ASYNC";
                }
                query["query"] = "INSERT INTO test VALUES ( " + SQ(cmdID) + ", " + SQ("cmd:" + to_string(cmdID) + ", " + query["writeConsistency"] + " sent to node:" + to_string(i)) + " );";

                // Ok, send.
                string result = brtester->executeWait(query);
            }
        }

        // Now see if they all match. If they don't, give them a few seconds to sync.
        int tries = 0;
        bool success = false;
        while (tries < 10) {
            vector<string> results(3);
            for (int i : {0, 1, 2}) {
                BedrockTester* brtester = tester->getBedrockTester(i);
                SData query("Query");
                query["writeConsistency"] = "ASYNC";
                query["query"] = "SELECT id, value FROM test ORDER BY id;";
                string result = brtester->executeWait(query);
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
        // Get the global tester object.
        tester = BedrockClusterTester::testers.front();

        recursive_mutex m;
        list <vector<pair<string,string>>> allResults;

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &allResults, &m](){
                BedrockTester* brtester = tester->getBedrockTester(i);

                // Let's make ourselves 20 commands to spam at each node.
                vector<SData> requests;
                int numCommands = 20;
                for (int j = 0; j < numCommands; j++) {
                    SData query("Query");
                    query["writeConsistency"] = "ASYNC";
                    query["query"] = "INSERT INTO test VALUES ( NULL, " + SQ("node: " + to_string(i) + ", cmd: " + to_string(j)) + " );";
                    requests.push_back(query);
                }

                // Ok, send them all!
                auto results = brtester->executeWaitMultiple(requests);

                SAUTOLOCK(m);
                allResults.push_back(results);
            });
        }

        for (thread& t : threads) {
            t.join();
        }

        for (auto resultList : allResults) {
            cout << "Result list:" << endl;
            for (auto resultPair : resultList) {
                cout << resultPair.first << ":" << resultPair.second << endl;
            }
            cout << endl;
        }
    }
} __b_ConflictSpamTest;
