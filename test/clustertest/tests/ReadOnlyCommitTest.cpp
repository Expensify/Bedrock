#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct ReadOnlyCommitTest : tpunit::TestFixture
{
    ReadOnlyCommitTest()
        : tpunit::TestFixture("ReadOnlyCommit", TEST(ReadOnlyCommitTest::completesWithoutInventingJournalEntries))
    {
    }

    STable state(BedrockTester& node)
    {
        return node.executeWaitVerifyContentTable(SData("getjournalteststate"));
    }

    uint64_t callbackCount(BedrockTester& node)
    {
        return SToUInt64(node.executeWaitVerifyContentTable(SData("getaftercommitcount")).at("afterCommitCount"));
    }

    void completesWithoutInventingJournalEntries()
    {
        BedrockClusterTester cluster(ClusterSize::THREE_NODE_CLUSTER, {}, {{"-journalDeleterBatchSize", "0"}});
        BedrockTester& leader = cluster.getTester(0);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        for (size_t i = 1; i < 3; ++i) {
            ASSERT_TRUE(cluster.getTester(i).waitForState("FOLLOWING"));
        }

        SData write("Query");
        write["Query"] = "INSERT INTO test VALUES(876580000, 'before no-op');";
        leader.executeWaitVerifyContent(write);
        const STable before = state(leader);
        uint64_t expectedCID = SToUInt64(before.at("commitCount"));
        for (size_t i = 0; i < 3; ++i) {
            auto& node = cluster.getTester(i);
            ASSERT_TRUE(node.waitForStatusTerm("commitCount", to_string(expectedCID)));
        }
        const uint64_t callbacksBefore = callbackCount(leader);
        const string journalRowsBefore = leader.readDB("SELECT COUNT(*) FROM journalEntries;");
        const bool nativeJournal = BedrockTester::ENABLE_HCTREE && BedrockTester::ENABLE_HCTREE_EXPERIMENTAL_MODE;

        for (uint64_t i = 1; i <= 3; ++i) {
            const auto responses = leader.executeWaitMultipleData({SData("readonlycommit")});
            ASSERT_EQUAL(responses.size(), 1ul);
            ASSERT_EQUAL(responses[0].methodLine, "200 OK");
            if (!nativeJournal) {
                ++expectedCID;
            }
            ASSERT_EQUAL(responses[0]["commitCount"], to_string(expectedCID));
            EXPECT_EQUAL(callbackCount(leader), callbacksBefore + i);
            if (nativeJournal) {
                const STable after = state(leader);
                for (const string key : {"commitCount", "hashCommitID", "hash"}) {
                    EXPECT_EQUAL(after.at(key), before.at(key));
                }
                EXPECT_EQUAL(leader.readDB("SELECT COUNT(*) FROM journalEntries;"), journalRowsBefore);
            }
        }

        // This waits for every registered writer, including handles used by the no-op commits, to finish.
        leader.executeWaitVerifyContent(SData("BlockWrites"), "200 Blocked", true);
        leader.executeWaitVerifyContent(SData("UnblockWrites"), "200 Unblocked", true);

        write["Query"] = "INSERT INTO test VALUES(876580001, 'after no-op');";
        leader.executeWaitVerifyContent(write);
        ++expectedCID;
        const STable afterWrite = state(leader);
        ASSERT_EQUAL(afterWrite.at("commitCount"), to_string(expectedCID));
        for (size_t i = 1; i < 3; ++i) {
            auto& follower = cluster.getTester(i);
            ASSERT_TRUE(follower.waitForStatusTerm("commitCount", to_string(expectedCID)));
            EXPECT_EQUAL(state(follower).at("hash"), afterWrite.at("hash"));
            EXPECT_EQUAL(follower.readDB("SELECT value FROM test WHERE id = 876580001;"), "after no-op");
        }
        EXPECT_EQUAL(leader.readDB("SELECT decompress(query) FROM journalEntries WHERE id = " + SQ(expectedCID) + ";"), write["Query"]);

        // An empty process phase can still become a real write during prepare.
        SData prepareWrite("readonlycommit");
        prepareWrite["writeInPrepare"] = "true";
        leader.executeWaitVerifyContent(prepareWrite);
        ++expectedCID;
        const STable afterPrepare = state(leader);
        ASSERT_EQUAL(afterPrepare.at("commitCount"), to_string(expectedCID));
        for (size_t i = 0; i < 3; ++i) {
            auto& node = cluster.getTester(i);
            ASSERT_TRUE(node.waitForStatusTerm("commitCount", to_string(expectedCID)));
            EXPECT_EQUAL(state(node).at("hash"), afterPrepare.at("hash"));
            EXPECT_EQUAL(node.readDB("SELECT COUNT(*) FROM test WHERE value = 'this is written in onPrepareHandler';"), "1");
        }
    }
} __ReadOnlyCommitTest;
