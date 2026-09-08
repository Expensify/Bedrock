/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    MassiveQueryTest.cpp
 * Path:    test/clustertest/tests/MassiveQueryTest.cpp
 *
 * INTENT
 *   Cluster test verifying that a large, long-running write ("bigquery")
 *   sent to a follower is escalated to the leader, committed, and replicated
 *   out to every other follower, confirmed here by polling for a matching
 *   commit count.
 *
 * OBJECTS
 *   MassiveQueryTest        - tpunit fixture, single free-standing test.
 *   MassiveQueryTest::test - sends the query to a follower, then polls a second follower's
 *                             `Status` until its commit count catches up.
 *   __MassiveQueryTest      - static instance that registers the fixture with tpunit.
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

struct MassiveQueryTest : tpunit::TestFixture
{
    MassiveQueryTest() : tpunit::TestFixture("MassiveQuery", TEST(MassiveQueryTest::test))
    {
    }

    void test()
    {
        BedrockClusterTester tester;
        // We're going to send a command to a follower, it should run on leader and get replicated.
        BedrockTester& brtester = tester.getTester(1);
        SData cmd("bigquery");
        cmd["processTimeout"] = "290000";
        auto r1 = brtester.executeWaitMultipleData({cmd})[0];
        uint64_t commitCount = 0;
        try {
            commitCount = stoull(r1["CommitCount"]);
        } catch (const invalid_argument& e) {
            cout << "invalid_argument parsing commitCount from: " << r1["CommitCount"] << endl;
        } catch (const out_of_range& e) {
            cout << "out_of_range parsing commitCount from: " << r1["CommitCount"] << endl;
        }
        uint64_t commitCount2 = 0;

        // Make sure the commit count is actually set.
        ASSERT_TRUE(commitCount);

        SData status("Status");
        for (size_t i = 0; i < 500; i++) {
            auto responseList = tester.getTester(2).executeWaitMultipleData({status});
            auto r2 = responseList[0];
            auto json = SParseJSONObject(r2.content);
            try {
                commitCount2 = stoull(json["CommitCount"]);
            } catch (const invalid_argument& e) {
                cout << "invalid_argument parsing commitCount2." << endl;
                cout << r2.serialize() << endl;
            } catch (const out_of_range& e) {
                cout << "out_of_range parsing commitCount2." << endl;
                cout << r2.serialize() << endl;
            }
            if (commitCount2 == commitCount) {
                break;
            }
            sleep(1);
        }

        ASSERT_EQUAL(commitCount, commitCount2);
    }
} __MassiveQueryTest;
