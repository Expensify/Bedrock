
#include "../BedrockClusterTester.h"

struct h_timingTest : tpunit::TestFixture {
    h_timingTest()
        : tpunit::TestFixture("h_timingTest",
                              TEST(h_timingTest::test)) { }

    BedrockClusterTester* tester;
    void test()
    {
        // Test write commands.
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();
        for (auto i : {0,1,2}) {
            BedrockTester* brtester = tester->getBedrockTester(i);

            // This just verifies that the dbupgrade table was created by TestPlugin.
            SData query("idcollision");
            query["writeConsistency"] = "ASYNC";
            query["value"] = "default";
            SData result = brtester->executeWaitData(query);
            /* Uncomment for inspection.
            for (const auto& row : result.nameValueMap) {
                cout << row.first << ":" << row.second << endl;
            }
            cout << endl;
            */

            uint64_t peekTime = SToUInt64(result["peekTime"]);
            uint64_t processTime = SToUInt64(result["processTime"]);
            uint64_t totalTime = SToUInt64(result["totalTime"]);

            // Only master is expected to have these set.
            if (i == 0) {
                ASSERT_GREATER_THAN(peekTime, 0);
                ASSERT_GREATER_THAN(processTime, 0);
            } else {
                ASSERT_EQUAL(peekTime, 0);
                ASSERT_EQUAL(processTime, 0);
            }

            ASSERT_LESS_THAN(peekTime + processTime, totalTime);

            if (i != 0) {
                // Extra data on slaves.
                uint64_t escalationTime = SToUInt64(result["escalationTime"]);
                uint64_t upstreamPeekTime = SToUInt64(result["upstreamPeekTime"]);
                uint64_t upstreamProcessTime = SToUInt64(result["upstreamProcessTime"]);
                uint64_t upstreamTotalTime = SToUInt64(result["upstreamTotalTime"]);

                ASSERT_GREATER_THAN(escalationTime, 0);
                ASSERT_GREATER_THAN(upstreamPeekTime, 0);
                ASSERT_GREATER_THAN(upstreamProcessTime, 0);
                ASSERT_GREATER_THAN(upstreamTotalTime, 0);

                ASSERT_LESS_THAN(escalationTime, totalTime);
                ASSERT_LESS_THAN(upstreamTotalTime, escalationTime);
                ASSERT_LESS_THAN(upstreamPeekTime + upstreamProcessTime, upstreamTotalTime);
            }
        }

        // Test read commands
        for (auto i : {0,1,2}) {
            BedrockTester* brtester = tester->getBedrockTester(i);

            // This just verifies that the dbupgrade table was created by TestPlugin.
            SData query("Query");
            query["query"] = "SELECT * FROM test;";
            SData result = brtester->executeWaitData(query);
            /* Uncomment for inspection.
            for (const auto& row : result.nameValueMap) {
                cout << row.first << ":" << row.second << endl;
            }
            cout << endl;
            */

            uint64_t peekTime = SToUInt64(result["peekTime"]);
            uint64_t processTime = SToUInt64(result["processTime"]);
            uint64_t totalTime = SToUInt64(result["totalTime"]);

            ASSERT_GREATER_THAN(peekTime, 0);
            ASSERT_EQUAL(processTime, 0);
            ASSERT_LESS_THAN(peekTime + processTime, totalTime);
        }
    }
} __h_timingTest;

