#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct StatusTest : tpunit::TestFixture
{
    StatusTest()
        : tpunit::TestFixture("Status",
                              BEFORE_CLASS(StatusTest::setup),
                              AFTER_CLASS(StatusTest::teardown),
                              TEST(StatusTest::status))
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    void status()
    {
        mutex m;

        // Send to each node simultaneously.
        list<thread> threads;
        vector<string> responses(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &responses, &m](){
                BedrockTester& brtester = tester->getTester(i);

                SData status("Status");
                status["writeConsistency"] = "ASYNC";

                // Ok, send them all!
                auto result = brtester.executeWaitVerifyContent(status);
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
                ASSERT_EQUAL(json["isLeader"], "true");
            } else {
                ASSERT_EQUAL(json["isLeader"], "false");
            }
            ASSERT_EQUAL(peers.size(), 2);
        }
    }
} __StatusTest;
