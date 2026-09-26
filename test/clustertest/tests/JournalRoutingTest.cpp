#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>
#include <test/clustertest/JournalTestHelper.h>

struct JournalRoutingTest : tpunit::TestFixture
{
    JournalRoutingTest()
        : tpunit::TestFixture("JournalRouting",
                              TEST(JournalRoutingTest::routesReadsAroundCutover),
                              TEST(JournalRoutingTest::freshDatabaseSkipsLegacy),
                              TEST(JournalRoutingTest::trimmingPreservesLegacySnapshot),
                              TEST(JournalRoutingTest::trimmingPreservesPromotionBoundary),
                              TEST(JournalRoutingTest::legacyWritesRemainVisible))
    {
    }

    const map<string, string> args = {
        {"-plugins", "db,testplugin/testplugin.so"},
        {"-maxJournalSize", "5"},
        {"-journalTables", "1"},
        {"-journalDeleterBatchSize", "0"},
    };

    JournalTestHelper::LegacyHistory seedLegacy(BedrockTester& node, bool hctree)
    {
        return JournalTestHelper::seedLegacyHistory(node.getArg("-db"), hctree, "routingData", 10, true);
    }

    STable run(BedrockTester& node, const string& op, uint64_t from = 1, uint64_t to = 0, int rounds = 1, int batchSize = 1)
    {
        SData command("journaltest");
        command["op"] = op;
        command["from"] = to_string(from);
        command["to"] = to_string(to);
        command["rounds"] = to_string(rounds);
        command["batchSize"] = to_string(batchSize);
        return SParseJSONObject(node.executeWaitVerifyContent(command));
    }

    string expectedRows(BedrockTester& node, uint64_t from, uint64_t to)
    {
        SQResult result;
        SASSERT(node.readDB("SELECT hex(query) || ':' || hash AS entry FROM journalEntries WHERE id >= " + SQ(from) +
                            (to ? " AND id <= " + SQ(to) : "") + " ORDER BY id;", result));
        string rows;
        for (const auto& row : result) {
            rows += row["entry"] + "\n";
        }
        return rows;
    }

    void verifySources(const STable& result, bool legacy, bool hct)
    {
        EXPECT_EQUAL(SContains(result.at("queries"), "FROM journal"), legacy);
        EXPECT_EQUAL(SContains(result.at("queries"), "FROM hct_journal"), hct);
    }

    void routesReadsAroundCutover()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }
        BedrockTester node(args, {}, 0, 0, 0, false);
        const auto history = seedLegacy(node, true);
        const uint64_t boundary = history.lastCommitID;
        const string& legacyHash = history.lastHash;
        ASSERT_GREATER_THAN(boundary, 1ull);
        node.startServer();
        const uint64_t latest = SToUInt64(node.readDB("SELECT MAX(id) FROM journalEntries;"));
        ASSERT_GREATER_THAN(latest, boundary);

        for (const auto& [from, to] : vector<pair<uint64_t, uint64_t>>{
            {2, boundary - 1}, {boundary, boundary}, {boundary, latest},
            {boundary - 1, boundary + 1}, {2, 0}, {boundary, 0}, {latest + 1, latest + 2},
        }) {
            const STable result = run(node, "range", from, to);
            EXPECT_EQUAL(result.at("rows"), expectedRows(node, from, to));
            verifySources(result, from < boundary, !to || to >= boundary);
        }
        for (const uint64_t id : vector<uint64_t>{2, boundary - 1, boundary, latest, latest + 1}) {
            const STable result = run(node, "commit", id);
            SQResult expected;
            ASSERT_TRUE(node.readDB("SELECT decompress(query) AS query, hash FROM journalEntries WHERE id = " + SQ(id) + ";", expected));
            EXPECT_EQUAL(result.at("found"), expected.empty() ? "false" : "true");
            if (!expected.empty()) {
                EXPECT_EQUAL(result.at("query"), expected[0]["query"]);
                EXPECT_EQUAL(result.at("hash"), expected[0]["hash"]);
            }
            verifySources(result, id < boundary, id >= boundary);
        }

        const STable legacy = run(node, "last", 1, boundary - 1);
        EXPECT_EQUAL(legacy.at("id"), to_string(boundary - 1));
        EXPECT_EQUAL(legacy.at("hash"), legacyHash);
        verifySources(legacy, true, false);
        const STable fallback = run(node, "last", 1, boundary);
        EXPECT_EQUAL(fallback.at("id"), legacy.at("id"));
        EXPECT_EQUAL(fallback.at("hash"), legacyHash);
        verifySources(fallback, true, true);
        const STable hct = run(node, "last", 1, latest);
        EXPECT_EQUAL(hct.at("id"), to_string(latest));
        verifySources(hct, false, true);
    }

    void freshDatabaseSkipsLegacy()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }
        BedrockTester node(args, {});
        const STable range = run(node, "range");
        EXPECT_EQUAL(range.at("rows"), expectedRows(node, 1, 0));
        verifySources(range, false, true);
        const STable paused = run(node, "trim", 1, 0, 1, 0);
        EXPECT_EQUAL(paused.at("tablesAfter"), "1");
        EXPECT_TRUE(paused.at("trimQueries").empty());
    }

    void trimmingPreservesLegacySnapshot()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }
        BedrockTester node(args, {}, 0, 0, 0, false);
        const auto history = seedLegacy(node, true);
        const uint64_t boundary = history.lastCommitID;
        const string& legacyHash = history.lastHash;
        ASSERT_GREATER_THAN(boundary, 1ull);
        node.startServer();
        const uint64_t before = SToUInt64(node.readDB("SELECT COUNT(*) FROM journalEntries WHERE id < " + SQ(boundary) + ";"));
        const STable paused = run(node, "trim", 1, 0, 1, 0);
        EXPECT_EQUAL(paused.at("tablesAfter"), "4");
        EXPECT_TRUE(paused.at("trimQueries").empty());
        const STable firstTrim = run(node, "trim");
        EXPECT_EQUAL(firstTrim.at("tablesBefore"), "4");
        EXPECT_FALSE(SContains(firstTrim.at("trimQueries"), "hct_journal"));
        EXPECT_EQUAL(SToUInt64(node.readDB("SELECT COUNT(*) FROM journalEntries WHERE id < " + SQ(boundary) + ";")), before - 3);

        for (int i = 100; i < 110; ++i) {
            SData write("Query");
            write["Query"] = "INSERT INTO routingData VALUES(" + SQ(i) + ", 'HC-Tree');";
            node.executeWaitVerifyContent(write);
        }
        const string retained = expectedRows(node, boundary - 2, boundary - 1);
        ASSERT_FALSE(retained.empty());
        const STable snapshot = run(node, "snapshot", boundary - 2, boundary - 1, 30);
        EXPECT_EQUAL(snapshot.at("tablesAfter"), "1");
        EXPECT_EQUAL(snapshot.at("before"), retained);
        EXPECT_EQUAL(snapshot.at("rows"), retained);
        EXPECT_EQUAL(snapshot.at("found"), "true");
        EXPECT_EQUAL(snapshot.at("id"), to_string(boundary - 1));
        EXPECT_EQUAL(snapshot.at("hash"), legacyHash);
        EXPECT_TRUE(snapshot.at("afterRollback").empty());
        const STable drained = run(node, "trim");
        EXPECT_EQUAL(drained.at("tablesBefore"), "1");
        EXPECT_FALSE(SContains(drained.at("trimQueries"), "FROM journal"));

        node.stopServer();
        node.startServer();
        const STable expired = run(node, "range", boundary - 2, boundary - 1);
        EXPECT_TRUE(expired.at("rows").empty());
        verifySources(expired, false, false);
        const STable noHash = run(node, "last", 1, boundary - 1);
        EXPECT_EQUAL(noHash.at("id"), "0");
        EXPECT_TRUE(noHash.at("hash").empty());
        verifySources(noHash, false, false);
        EXPECT_EQUAL(run(node, "trim").at("tablesBefore"), "1");
    }

    void trimmingPreservesPromotionBoundary()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }
        BedrockTester node(args, {}, 0, 0, 0, false);
        const auto history = seedLegacy(node, true);
        const uint64_t boundary = history.lastCommitID;
        ASSERT_GREATER_THAN(boundary, 1ull);
        node.startServer();
        for (int i = 100; i < 110; ++i) {
            SData write("Query");
            write["Query"] = "INSERT INTO routingData VALUES(" + SQ(i) + ", 'advance retention');";
            node.executeWaitVerifyContent(write);
        }

        SData trim("journaltest");
        trim["op"] = "trim";
        trim["batchSize"] = "100";
        trim["table"] = "3";
        node.executeWaitVerifyContent(trim);
        ASSERT_EQUAL(node.readDB("SELECT MIN(cid) FROM hct_journal;"), to_string(boundary));

        // The shard holding the newest legacy commit must retain that entry until the other shards are empty.
        const string legacyMax = "SELECT MAX(id) FROM (SELECT id FROM journal UNION ALL SELECT id FROM journal0000 UNION ALL SELECT id FROM journal0001);";
        trim["table"] = "1";
        node.executeWaitVerifyContent(trim);
        ASSERT_EQUAL(node.readDB(legacyMax), to_string(boundary - 1));
        node.stopServer();
        node.startServer();
        ASSERT_TRUE(node.waitForState("LEADING"));

        trim["batchSize"] = "1";
        for (int batch = 0; batch < 50; ++batch) {
            trim["table"] = to_string(batch);
            node.executeWaitVerifyContent(trim);
            const string lastLegacy = node.readDB(legacyMax);
            if (!lastLegacy.empty()) {
                ASSERT_EQUAL(SToUInt64(lastLegacy) + 1, SToUInt64(node.readDB("SELECT MIN(cid) FROM hct_journal;")));
            }
        }
        EXPECT_TRUE(node.readDB(legacyMax).empty());
        SQResult bounds;
        ASSERT_TRUE(node.readDB("SELECT MIN(cid) AS oldest, MAX(cid) AS newest, COUNT(*) AS count FROM hct_journal;", bounds));
        ASSERT_EQUAL(bounds.size(), 1ul);
        EXPECT_EQUAL(SToUInt64(bounds[0]["oldest"]), SToUInt64(bounds[0]["newest"]) - 5);
        EXPECT_EQUAL(bounds[0]["count"], "6");
    }

    void legacyWritesRemainVisible()
    {
        BedrockTester node(args, {}, 0, 0, 0, false);
        const auto history = seedLegacy(node, false);
        ASSERT_GREATER_THAN(history.lastCommitID, 1ull);
        node.startServer();
        const uint64_t start = SToUInt64(node.readDB("SELECT MAX(id) FROM journalEntries;"));
        for (int i = 100; i < 103; ++i) {
            SData write("Query");
            write["Query"] = "INSERT INTO routingData VALUES(" + SQ(i) + ", 'WAL2');";
            node.executeWaitVerifyContent(write);
        }
        const STable rows = run(node, "range", start + 1);
        EXPECT_EQUAL(rows.at("rows"), expectedRows(node, start + 1, 0));
        EXPECT_FALSE(rows.at("rows").empty());
        verifySources(rows, true, false);
        const STable last = run(node, "last", 1, start + 3);
        EXPECT_EQUAL(last.at("id"), to_string(start + 3));
        verifySources(last, true, false);
        EXPECT_EQUAL(run(node, "trim", 1, 0, 30).at("tablesAfter"), "3");
    }
} __JournalRoutingTest;
