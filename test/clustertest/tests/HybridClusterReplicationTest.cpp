#include <libstuff/JSON/Utils.h>
#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>

#include <thread>

struct HybridClusterReplicationTest : tpunit::TestFixture
{
    HybridClusterReplicationTest()
        : tpunit::TestFixture("HybridClusterReplication",
                              TEST(HybridClusterReplicationTest::testReplication),
                              TEST(HybridClusterReplicationTest::testFollowerUpgrade))
    {
    }

    enum class Backend
    {
        EXPERIMENTAL_HCTREE,
        HCTREE,
        WAL2,
    };

    struct Cluster
    {
        vector<unique_ptr<BedrockTester>> nodes;

        explicit Cluster(Backend leaderBackend)
        {
            const vector<Backend> backends{leaderBackend, Backend::EXPERIMENTAL_HCTREE, Backend::HCTREE, Backend::WAL2, leaderBackend};
            for (size_t i = 0; i < backends.size(); ++i) {
                map<string, string> args = {
                    {"-nodeName", "hybrid_node_" + to_string(i)},
                    {"-priority", i == 4 ? "0" : to_string(100 - i * 10)},
                    {"-plugins", "db"},
                    {"-journalDeleterBatchSize", "0"},
                };
                if (backends[i] != Backend::WAL2) {
                    args["-newDBsUseHctree"] = "";
                }
                if (backends[i] == Backend::EXPERIMENTAL_HCTREE) {
                    args["-hctreeExperimentalMode"] = "";
                }
                nodes.push_back(make_unique<BedrockTester>(args, list<string>{}, 0, 0, 0, false));
                nodes.back()->remoteMode = true;
            }
            for (size_t i = 0; i < nodes.size(); ++i) {
                list<string> peers;
                for (size_t j = 0; j < nodes.size(); ++j) {
                    if (i != j) {
                        peers.push_back(nodes[j]->getArg("-nodeHost") + "?nodeName=" + nodes[j]->getArg("-nodeName") +
                                        (j == 4 ? "&Permafollower=true" : ""));
                    }
                }
                nodes[i]->updateArgs({{"-peerList", SComposeList(peers, ",")}});
            }
            for (auto& node : nodes) {
                node->startServerInBackground();
            }
        }

        ~Cluster()
        {
            vector<thread> threads;
            for (size_t i = 1; i < nodes.size(); ++i) {
                threads.emplace_back([this, i]() {
                    nodes[i]->stopServer();
                });
            }
            for (auto& thread : threads) {
                thread.join();
            }
            nodes[0]->stopServer();
        }

        bool initialize()
        {
            for (size_t i = 0; i < nodes.size(); ++i) {
                if (!nodes[i]->waitForState(i == 0 ? "LEADING" : "FOLLOWING")) {
                    return false;
                }
            }
            SData create("Query");
            create["Query"] = "CREATE TABLE hybridReplication (id INTEGER PRIMARY KEY, value TEXT);";
            nodes[0]->executeWaitVerifyContent(create);
            return true;
        }
    };

    uint64_t commitCount(BedrockTester& node)
    {
        return SToUInt64(JSON::Utils::toSTable(JSON::Value::parse(node.executeWaitVerifyContent(SData("Status"), "200", true)))["commitCount"]);
    }

    uint64_t writeCommits(Cluster& cluster, uint64_t first = 0)
    {
        BedrockTester& leader = *cluster.nodes[0];
        const uint64_t before = commitCount(leader);
        for (uint64_t i = first; i < first + 100; ++i) {
            SData write("Query");
            write["Query"] = "INSERT INTO hybridReplication VALUES (" + SQ(i) + ", " + SQ("value-" + to_string(i)) + ");";
            leader.executeWaitVerifyContent(write);
        }
        const uint64_t after = commitCount(leader);
        EXPECT_EQUAL(after, before + 100);
        return after;
    }

    void verifyRows(Cluster& cluster, uint64_t count, uint64_t target)
    {
        for (auto& node : cluster.nodes) {
            ASSERT_TRUE(node->waitForStatusTerm("commitCount", to_string(target)));
            SQResult rows;
            ASSERT_TRUE(node->readDB("SELECT id, value FROM hybridReplication ORDER BY id;", rows));
            ASSERT_EQUAL(rows.size(), count);
            for (uint64_t i = 0; i < count; ++i) {
                EXPECT_EQUAL(rows[i]["id"], to_string(i));
                EXPECT_EQUAL(rows[i]["value"], "value-" + to_string(i));
            }
        }
    }

    void testReplication()
    {
        if (BedrockTester::ENABLE_HCTREE) {
            return;
        }
        for (const auto backend : {Backend::EXPERIMENTAL_HCTREE, Backend::HCTREE, Backend::WAL2}) {
            Cluster cluster(backend);
            ASSERT_TRUE(cluster.initialize());
            const uint64_t target = writeCommits(cluster);
            verifyRows(cluster, 100, target);
        }
    }

    void testFollowerUpgrade()
    {
        if (BedrockTester::ENABLE_HCTREE) {
            return;
        }
        Cluster cluster(Backend::EXPERIMENTAL_HCTREE);
        ASSERT_TRUE(cluster.initialize());
        const uint64_t boundary = writeCommits(cluster);
        verifyRows(cluster, 100, boundary);

        BedrockTester& follower = *cluster.nodes[2];
        ASSERT_EQUAL(follower.readDB("SELECT COUNT(*) FROM sqlite_schema WHERE name = 'hct_journal';"), "0");
        const string journalQuery = "SELECT id, hex(query) AS query, hash FROM journalEntries ORDER BY id;";
        SQResult before;
        ASSERT_TRUE(follower.readDB(journalQuery, before));

        follower.stopServer();
        follower.updateArgs({{"-hctreeExperimentalMode", ""}});
        follower.startServer();
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));
        ASSERT_TRUE(follower.waitForStatusTerm("commitCount", to_string(boundary)));
        EXPECT_EQUAL(follower.readDB("SELECT MIN(cid) FROM hct_journal;"), to_string(boundary));
        SQResult preserved;
        ASSERT_TRUE(follower.readDB("SELECT id, hex(query) AS query, hash FROM journalEntries WHERE id <= " + SQ(boundary) + " ORDER BY id;", preserved));
        EXPECT_EQUAL(preserved.serializeToText(), before.serializeToText());

        const uint64_t target = writeCommits(cluster, 100);
        verifyRows(cluster, 200, target);
        EXPECT_EQUAL(follower.readDB("SELECT COUNT(*) FROM hct_journal;"), "101");
        EXPECT_EQUAL(follower.readDB("SELECT MAX(cid) FROM hct_journal;"), to_string(target));
        EXPECT_EQUAL(follower.readDB("SELECT COUNT(*) FROM hct_journal WHERE cid > " + SQ(boundary) + " AND snapshot != cid - 1;"), "0");
        SQResult expected, actual;
        ASSERT_TRUE(cluster.nodes[0]->readDB(journalQuery, expected));
        ASSERT_TRUE(follower.readDB(journalQuery, actual));
        EXPECT_EQUAL(actual.serializeToText(), expected.serializeToText());
    }
} __HybridClusterReplicationTest;
