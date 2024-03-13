#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct MassiveQueryTest : tpunit::TestFixture {

    MassiveQueryTest() : tpunit::TestFixture("MassiveQuery", TEST(MassiveQueryTest::test)) { }

    void test()
    {
        BedrockClusterTester tester;
        // We're going to send a command to a follower, it should run on leader and get replicated.
        BedrockTester& brtester = tester.getTester(1);
        SData cmd("bigquery");
        cmd["processTimeout"] = "290000";
        cmd["writeConsistency"] = "ASYNC";
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
