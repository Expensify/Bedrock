#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct TimingTest : tpunit::TestFixture {
    TimingTest()
        : tpunit::TestFixture("Timing", TEST(TimingTest::test)) { }

    void test()
    {
        BedrockClusterTester tester;
        // Test write commands.
        for (auto i : {0,1,2}) {
            BedrockTester& brtester = tester.getTester(i);

            // This just verifies that the dbupgrade table was created by TestPlugin.
            SData query("idcollision h");
            query["writeConsistency"] = "ASYNC";
            query["value"] = "default";
            int retries = 3;
            SData result;
            while (retries) {
                auto results = brtester.executeWaitMultipleData({query});
                result = results[0];
                if (result.isSet("totalTime")) {
                    break;
                } else {
                    sleep(1);
                    retries--;
                    continue;
                }
            }
            /* Uncomment for inspection.
            for (const auto& row : result.nameValueMap) {
                cout << "[TimingTest] " << row.first << ":" << row.second << endl;
            }
            cout << "[TimingTest] " << endl;
            */

            uint64_t peekTime = SToUInt64(result["peekTime"]);
            uint64_t processTime = SToUInt64(result["processTime"]);
            uint64_t totalTime = SToUInt64(result["totalTime"]);

            // Leader should have peek and process times, followers only peek.
            if (i == 0) {
                if (peekTime <= 0 || processTime <= 0) {
                    cout << "[TimingTest] peekTime: " << peekTime << endl;
                    cout << "[TimingTest] processTime: " << processTime << endl;
                    cout << "[TimingTest] totalTime: " << totalTime << endl;
                    cout << "[TimingTest] " << result.serialize() << endl;
                }
                ASSERT_GREATER_THAN(peekTime, 0);
                ASSERT_GREATER_THAN(processTime, 0);
            } else {
                ASSERT_GREATER_THAN(peekTime, 0);
                ASSERT_EQUAL(processTime, 0);
            }

            if (peekTime + processTime >= totalTime) {
                // These are just blank in the failure case.
                cout << "[TimingTest] peekTime: " << peekTime << endl;
                cout << "[TimingTest] processTime: " << processTime << endl;
                cout << "[TimingTest] totalTime: " << totalTime << endl;
                cout << "[TimingTest] " << result.serialize() << endl;
            }

            ASSERT_LESS_THAN(peekTime + processTime, totalTime);

            if (i != 0) {
                // Extra data on followers.
                uint64_t upstreamPeekTime = SToUInt64(result["upstreamPeekTime"]);
                uint64_t upstreamProcessTime = SToUInt64(result["upstreamProcessTime"]);
                uint64_t upstreamTotalTime = SToUInt64(result["upstreamTotalTime"]);

                ASSERT_GREATER_THAN(upstreamPeekTime, 0);
                ASSERT_GREATER_THAN(upstreamProcessTime, 0);
                ASSERT_GREATER_THAN(upstreamTotalTime, 0);

                ASSERT_LESS_THAN(upstreamPeekTime + upstreamProcessTime, upstreamTotalTime);
            }
        }

        // Test read commands
        for (auto i : {0,1,2}) {
            BedrockTester& brtester = tester.getTester(i);

            // This just verifies that the dbupgrade table was created by TestPlugin.
            SData query("Query");
            query["query"] = "SELECT * FROM test;";
            auto results = brtester.executeWaitMultipleData({query});
            auto result = results[0];
            /* Uncomment for inspection.
            for (const auto& row : result.nameValueMap) {
                cout << "[TimingTest] " << row.first << ":" << row.second << endl;
            }
            cout << "[TimingTest.cpp]" << endl;
            */

            uint64_t peekTime = SToUInt64(result["peekTime"]);
            uint64_t processTime = SToUInt64(result["processTime"]);
            uint64_t totalTime = SToUInt64(result["totalTime"]);

            ASSERT_GREATER_THAN(peekTime, 0);
            ASSERT_EQUAL(processTime, 0);
            ASSERT_LESS_THAN(peekTime + processTime, totalTime);
        }
    }
} __TimingTest;

