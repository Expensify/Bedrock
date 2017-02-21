#include "../BedrockClusterTester.h"

struct f_HTTPSTest : tpunit::TestFixture {
    f_HTTPSTest()
        : tpunit::TestFixture("f_HTTPSTest",
                              TEST(f_HTTPSTest::test)) { }

    BedrockClusterTester* tester;
    void test()
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

                SData status("testcommand");
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

        for (string response: responses) {
            cout << response << endl;
        }
    }
} __f_HTTPSTest;
