#include "../BedrockClusterTester.h"

struct ConflictSpamTest : tpunit::TestFixture {
    ConflictSpamTest()
        : tpunit::TestFixture("ConflictSpam",
                              TEST(ConflictSpamTest::spam)) { }

    /* What's a conflict spam test? The main point of this test is to make sure we have lots of conflicting commits
     * coming in to the whole cluster, so that we can make sure they all eventually get committed and replicated in a
     * sane way. This is supposed to be a "worst case scenario" test where we can verify that the database isn't
     * corrupted or anything else horrible happens even in less-than-ideal circumstances.
     */

    BedrockClusterTester* tester;

    void spam()
    {
        // Get the global tester object.
        tester = BedrockClusterTester::testers.front();

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        #if 0
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i](){
                BedrockTester* brtester = tester->getBedrockTester(i);

                // Let's make ourselves 20 commands to spam at each node.
                vector<SData> requests;
                int numCommands = 20;
                for (int i = 0; i < numCommands; i++) {
                    SData query("Query");
                    query["writeConsistency"] = "ASYNC";
                    query["query"] = "INSERT INTO stuff VALUES ( NULL, " + SQ(i) + " );";
                    requests.push_back(query);
                }

                // Ok, send them all!
                auto results = brtester->executeWaitMultiple(requests);

                int success = 0;
                int failure = 0;

                for (auto& row : results) {
                    if (SToInt(row.first) == 200) {
                        success++;
                    } else {
                        failure++;
                    }
                }

                ASSERT_EQUAL(success, numCommands);
            });
        }
        #endif

        for (thread& t : threads) {
            t.join();
        }
    }
} __ConflictSpamTest;
