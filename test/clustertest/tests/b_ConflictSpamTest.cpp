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

        // Send a write command to each node in the cluster. See that we get a result back.
        for (int i : {0, 1, 2}) {
            BedrockTester* brtester = tester->getBedrockTester(i);
            SData query("Query");
            query["writeConsistency"] = "ASYNC";
            query["query"] = "INSERT INTO test VALUES ( NULL, " + SQ("cmd:" + to_string(i) + " sent to node:" + to_string(i)) + " );";

            // Ok, send.
            string result = brtester->executeWait(query);
        }
    }

    void spam()
    {
    #if 0
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
                for (int i = 0; i < numCommands; i++) {
                    SData query("Query");
                    query["writeConsistency"] = "ASYNC";
                    query["query"] = "INSERT INTO test VALUES ( NULL, " + SQ(i) + " );";
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
        #endif
    }
} __b_ConflictSpamTest;
