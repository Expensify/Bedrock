#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/clustertest/BedrockClusterTester.h>

struct HCTreeJournalModeTest : tpunit::TestFixture
{
    HCTreeJournalModeTest()
        : tpunit::TestFixture("HCTreeJournalMode",
                              TEST(HCTreeJournalModeTest::leaderFailover),
                              TEST(HCTreeJournalModeTest::trimsOldestEntries))
    {
    }

    uint64_t commitCount(BedrockTester& node)
    {
        return SToUInt64(SParseJSONObject(node.executeWaitVerifyContent(SData("Status")))["commitCount"]);
    }

    void verifyHCTreeCommit(BedrockTester& node, uint64_t cid)
    {
        SQResult hct;
        ASSERT_TRUE(node.readDB("SELECT COUNT(*) FROM hct_journal WHERE cid = " + SQ(cid) + ";", hct));
        ASSERT_EQUAL(hct[0][0], "1");

        SQResult legacyTables;
        ASSERT_TRUE(node.readDB("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'journal%';", legacyTables));
        for (const auto& row : legacyTables) {
            SQResult legacy;
            ASSERT_TRUE(node.readDB("SELECT COUNT(*) FROM " + row[0] + " WHERE id = " + SQ(cid) + ";", legacy));
            ASSERT_EQUAL(legacy[0][0], "0");
        }
    }

    void leaderFailover()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }

        BedrockClusterTester cluster(ClusterSize::THREE_NODE_CLUSTER);
        BedrockTester& first = cluster.getTester(0);
        BedrockTester& second = cluster.getTester(1);
        BedrockTester& third = cluster.getTester(2);
        ASSERT_TRUE(first.waitForState("LEADING"));
        ASSERT_TRUE(second.waitForState("FOLLOWING"));
        ASSERT_TRUE(third.waitForState("FOLLOWING"));

        SData write("Query");
        write["Query"] = "INSERT INTO test VALUES(876543210, 'first leader');";
        first.executeWaitVerifyContent(write, "200");
        uint64_t firstCID = commitCount(first);
        ASSERT_TRUE(second.waitForStatusTerm("commitCount", to_string(firstCID)));
        ASSERT_TRUE(third.waitForStatusTerm("commitCount", to_string(firstCID)));
        verifyHCTreeCommit(first, firstCID);
        verifyHCTreeCommit(second, firstCID);

        cluster.stopNode(0);
        ASSERT_TRUE(second.waitForState("LEADING"));
        write["Query"] = "INSERT INTO test VALUES(876543211, 'second leader');";
        second.executeWaitVerifyContent(write, "200");
        uint64_t secondCID = commitCount(second);
        ASSERT_EQUAL(secondCID, firstCID + 1);
        ASSERT_TRUE(third.waitForStatusTerm("commitCount", to_string(secondCID)));
        verifyHCTreeCommit(second, secondCID);
        verifyHCTreeCommit(third, secondCID);

        cluster.startNode(0);
        ASSERT_TRUE(first.waitForState("LEADING"));
        ASSERT_TRUE(first.waitForStatusTerm("commitCount", to_string(secondCID)));
        verifyHCTreeCommit(first, secondCID);
    }

    void trimsOldestEntries()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }

        BedrockClusterTester cluster(ClusterSize::THREE_NODE_CLUSTER, {},
                                     {{"-maxJournalSize", "5"}, {"-journalTables", "0"}, {"-journalDeleterBatchSize", "10"}});
        BedrockTester& leader = cluster.getTester(0);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        for (int i = 0; i < 30; ++i) {
            SData write("Query");
            write["Query"] = "INSERT INTO test VALUES(" + SQ(876550000 + i) + ", 'trim');";
            leader.executeWaitVerifyContent(write, "200");
        }

        SQResult bounds;
        uint64_t deadline = STimeNow() + 5'000'000;
        do
        {
            bounds.clear();
            ASSERT_TRUE(leader.readDB("SELECT MIN(cid), MAX(cid) FROM hct_journal;", bounds));
            if (SToUInt64(bounds[0][0]) > 1) {
                break;
            }
            usleep(50'000);
        } while (STimeNow() < deadline);
        ASSERT_GREATER_THAN(SToUInt64(bounds[0][0]), 1ull);
        ASSERT_EQUAL(SToUInt64(bounds[0][1]), commitCount(leader));
        ASSERT_TRUE(leader.waitForState("LEADING"));
    }
} __HCTreeJournalModeTest;
