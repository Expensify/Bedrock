#include "../BedrockClusterTester.h"

struct c_StatusTest : tpunit::TestFixture {
    c_StatusTest()
        : tpunit::TestFixture("c_StatusTest",
                              TEST(c_StatusTest::status)) { }

    BedrockClusterTester* tester;
    void status()
    {
        return;
        // Get the global tester object.
        tester = BedrockClusterTester::testers.front();

        mutex m;

        // Send to each node simultaneously.
        list<thread> threads;
        vector<string> responses(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &responses, &m](){
                BedrockTester* brtester = tester->getBedrockTester(i);

                SData status("Status");
                status["writeConsistency"] = "ASYNC";

                // Ok, send them all!
                auto result = brtester->executeWait(status);
                lock_guard<decltype(m)> lock(m);
                responses[i] = result;
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        for (int i = 0; i < 3; i++) {
            STable json = SParseJSONObject(responses[i]);
            auto peers = SParseJSONArray(json["peerList"]);
            if (i == 0) {
                ASSERT_EQUAL(json["isMaster"], "true");
            } else {
                ASSERT_EQUAL(json["isMaster"], "false");
            }
            ASSERT_EQUAL(peers.size(), 2);
        }
    }
} __c_StatusTest;
