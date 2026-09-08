/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    StatusTest.cpp
 * Path:    test/clustertest/tests/StatusTest.cpp
 *
 * INTENT
 *   Cluster test verifying that a `Status` command sent concurrently to
 *   every node in a 3-node cluster reports the correct leader/follower role
 *   and a peer list of the expected size from each node's own perspective.
 *
 * OBJECTS
 *   StatusTest              - tpunit fixture; brings up a default cluster.
 *   StatusTest::status      - the test: fires `Status` at all three nodes in parallel, one thread per node.
 *   __StatusTest            - static instance that registers the fixture with tpunit.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with sibling test files.
 * ─────────────────────────────────────────────────────────────────────*/
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
