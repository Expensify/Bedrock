#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

struct JournalRoutingTest : tpunit::TestFixture
{
    JournalRoutingTest()
        : tpunit::TestFixture("JournalRouting",
                              TEST(JournalRoutingTest::routesReadsAroundCutover),
                              TEST(JournalRoutingTest::freshDatabaseSkipsLegacy),
                              TEST(JournalRoutingTest::trimmingPreservesLegacySnapshot),
                              TEST(JournalRoutingTest::legacyWritesRemainVisible))
    {
    }

    const map<string, string> args = {
        {"-plugins", "db,testplugin/testplugin.so"},
        {"-maxJournalSize", "5"},
        {"-journalTables", "1"},
        {"-journalDeleterBatchSize", "0"},
    };

    void seedLegacy(BedrockTester& node, bool hctree, uint64_t& boundary, string& hash)
    {
        ASSERT_FALSE(SQLite::hctreeExperimentalMode);
        SQLite db(node.getArg("-db"), 1000, 1000, 1, 0, hctree);
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.write("CREATE TABLE routingData(id INTEGER PRIMARY KEY, value TEXT);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        for (int i = 0; i < 10; ++i) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.write("INSERT INTO routingData VALUES(" + SQ(i) + ", 'legacy');"));
            ASSERT_TRUE(db.prepare());
            ASSERT_EQUAL(db.commit(), SQLITE_OK);
        }
        hash = db.getCommittedHash();
        // Moving this blank baseline must leave the agreement hash in legacy history.
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.prepare(nullptr, nullptr, chrono::hours(24), nullptr, ""));
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        boundary = db.getCommitCount();
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
        uint64_t boundary = 0;
        string legacyHash;
        seedLegacy(node, true, boundary, legacyHash);
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
        uint64_t boundary = 0;
        string legacyHash;
        seedLegacy(node, true, boundary, legacyHash);
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

    void legacyWritesRemainVisible()
    {
        BedrockTester node(args, {}, 0, 0, 0, false);
        uint64_t boundary = 0;
        string hash;
        seedLegacy(node, false, boundary, hash);
        ASSERT_GREATER_THAN(boundary, 1ull);
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
