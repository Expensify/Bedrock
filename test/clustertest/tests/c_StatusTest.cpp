#include "../BedrockClusterTester.h"

struct c_StatusTest : tpunit::TestFixture {
    c_StatusTest()
        : tpunit::TestFixture("b_ConflictSpam",
                              TEST(c_StatusTest::status)) { }

    BedrockClusterTester* tester;
    void status()
    {
        // Get the global tester object.
        tester = BedrockClusterTester::testers.front();

        mutex m;

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        list<string> responses;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &responses, &m](){
                BedrockTester* brtester = tester->getBedrockTester(i);

                SData status("Status");
                status["writeConsistency"] = "ASYNC";

                // Ok, send them all!
                auto result = brtester->executeWait(status);
                lock_guard<decltype(m)> lock(m);
                responses.push_back(result);
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        // TODO: Verify peers, once that works in Status.
    }
} __c_StatusTest;
