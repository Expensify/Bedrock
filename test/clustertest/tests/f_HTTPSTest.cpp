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

        // Send one request to verify that it works.
        BedrockTester* brtester = tester->getBedrockTester(0);

        SData status("sendrequest");
        auto result = brtester->executeWait(status);
        ASSERT_TRUE(result.size() > 10);
#if 0
        mutex m;

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        list<string> responses;
        // Only send to master.
        // for (int i : {0, 1, 2}) {
        for (int i : {0}) {
            threads.emplace_back([this, i, &responses, &m](){
                BedrockTester* brtester = tester->getBedrockTester(i);

                SData status("sendrequest");
                // TODO: Make this work as well, at least on master.
                // status["writeConsistency"] = "ASYNC";

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
#endif
    }
} __f_HTTPSTest;
