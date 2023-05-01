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
        cmd["writeConsistency"] = "ASYNC";
        auto r1 = brtester.executeWaitMultipleData({cmd})[0];
        cout << r1.serialize() << endl;
        cout << "QuerySize: " << r1["QuerySize"] << endl;
        uint64_t commitCount = stoull(r1["CommitCount"]);
        uint64_t commitCount2 = 0;

        SData status("Status");
        for (size_t i = 0; i < 60; i++) {
            auto r2 = tester.getTester(2).executeWaitMultipleData({status})[0];
            commitCount2 = stoull(SParseJSONObject(r2.content)["CommitCount"]);
            if (commitCount2 == commitCount) {
                cout << "Commit counts match at " << commitCount << endl;
                break;
            }
        }

        ASSERT_EQUAL(commitCount, commitCount2);
    }
} __MassiveQueryTest;
