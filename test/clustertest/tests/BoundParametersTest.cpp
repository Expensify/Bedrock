#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/clustertest/BedrockClusterTester.h>
#include <test/lib/RemoteSQLite.h>

// Verifies that a leader write using named bound parameters replicates correctly to followers — both
// that the row contents round-trip identically and that the journal text on each follower contains the
// inlined SQL literal (not the `:placeholder` form). A regression here was the divergence the bot
// flagged in PR #2600 review: leader journals raw SQL with placeholders, follower replays them with
// no params map, placeholders bind to NULL, leader/follower hashes diverge, cluster hangs at quorum.
struct BoundParametersTest : tpunit::TestFixture
{
    BoundParametersTest()
        : tpunit::TestFixture("BoundParameters",
                              BEFORE_CLASS(BoundParametersTest::setup),
                              AFTER_CLASS(BoundParametersTest::teardown),
                              TEST(BoundParametersTest::testBoundParamReplication),
                              TEST(BoundParametersTest::testRemoteSQLiteForwardsParams))
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester(ClusterSize::THREE_NODE_CLUSTER);
    }

    void teardown()
    {
        delete tester;
    }

    SQResult queryServer(BedrockTester& node, const string& sql)
    {
        SData command("Query");
        command["Format"] = "json";
        command["Query"] = sql;
        auto results = node.executeWaitMultipleData({command});
        SQResult result;
        if (results[0].methodLine == "200 OK" && !results[0].content.empty()) {
            result.deserialize(results[0].content);
        }
        return result;
    }

    uint64_t getCommitCount(BedrockTester& node)
    {
        SData status("Status");
        string response = node.executeWaitVerifyContent(status);
        STable json = SParseJSONObject(response);
        return SToUInt64(json["commitCount"]);
    }

    // Reads the journal `query` column for a given commit ID across all journal tables, decompressing on
    // the fly so the returned text is the original SQL regardless of whether journal compression is on.
    string readJournalText(BedrockTester& node, uint64_t commitID)
    {
        SData listCmd("Query");
        listCmd["Format"] = "json";
        listCmd["Query"] = "SELECT tbl_name FROM sqlite_master WHERE tbl_name LIKE 'journal%' ORDER BY tbl_name;";
        auto listResults = node.executeWaitMultipleData({listCmd});
        SQResult tables;
        tables.deserialize(listResults[0].content);

        string sql;
        for (size_t i = 0; i < tables.size(); i++) {
            if (!sql.empty()) {
                sql += " UNION ";
            }
            sql += "SELECT decompress(query) FROM " + tables[i][0] + " WHERE id = " + SQ(commitID);
        }
        sql += ";";

        SData command("Query");
        command["Format"] = "json";
        command["Query"] = sql;
        auto results = node.executeWaitMultipleData({command});
        if (results[0].methodLine != "200 OK" || results[0].content.empty()) {
            return "";
        }
        SQResult journal;
        journal.deserialize(results[0].content);
        return journal.empty() ? "" : journal[0][0];
    }

    void testBoundParamReplication()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower1 = tester->getTester(1);
        BedrockTester& follower2 = tester->getTester(2);

        // Issue a write that uses named bound parameters on the leader. The value is distinctive so we
        // can grep for it in the journal text on each node and confirm it was inlined as an SQL literal.
        const string distinctiveValue = "bound-param-replication-marker-7f3";
        SData cmd("writebound");
        cmd["value"] = distinctiveValue;
        leader.executeWaitVerifyContent(cmd, "200");

        // Wait for the followers to catch up to the commit that contains our write. If the leader/follower
        // hash divergence bug were still present the leader would never reach quorum and this would time
        // out, so just getting past this point is most of the test.
        uint64_t commit = getCommitCount(leader);
        ASSERT_TRUE(follower1.waitForStatusTerm("commitCount", to_string(commit)));
        ASSERT_TRUE(follower2.waitForStatusTerm("commitCount", to_string(commit)));

        // Round-trip check: every node should have a row with the distinctive value. If the leader sent
        // raw `:value` SQL and the follower ran it with no params map, the value would have ended up as
        // NULL on the follower and this would fail.
        for (int i = 0; i < 3; i++) {
            BedrockTester& node = tester->getTester(i);
            SQResult result = queryServer(node, "SELECT value FROM test WHERE value = " + SQ(distinctiveValue) + ";");
            ASSERT_EQUAL(result.size(), (size_t) 1);
            ASSERT_EQUAL(result[0][0], distinctiveValue);
        }

        // Journal text check: every node's journal entry for this commit should contain the inlined
        // literal `'<value>'` and must NOT contain the placeholder names `:id` or `:value`. This
        // confirms _writeIdempotent journaled the expanded form rather than the raw placeholder SQL.
        for (int i = 0; i < 3; i++) {
            BedrockTester& node = tester->getTester(i);
            string journalSql = readJournalText(node, commit);
            ASSERT_FALSE(journalSql.empty());
            ASSERT_TRUE(SContains(journalSql, "'" + distinctiveValue + "'"));
            ASSERT_FALSE(SContains(journalSql, ":id"));
            ASSERT_FALSE(SContains(journalSql, ":value"));
            cout << journalSql << endl;
        }
    }

    // Exercises the RemoteSQLite forwarding path end-to-end with named bound parameters: write through
    // RemoteSQLite, read back through RemoteSQLite, and verify the value didn't bind to NULL on the
    // server. If the `sql-param-<name>` header plumbing breaks (or if the DB plugin stops threading
    // params into db.read/db.write) the row inserts as NULL and this asserts immediately.
    void testRemoteSQLiteForwardsParams()
    {
        BedrockTester& leader = tester->getTester(0);
        RemoteSQLite remote(&leader);

        const int64_t id = 424242;
        const string value = "remote-sqlite-bound-params-marker";
        map<string, SQliteParameter> params = {
            {":id", SQliteParameter::i(id)},
            {":value", SQliteParameter::text(value)},
        };
        ASSERT_TRUE(remote.write("INSERT INTO test (id, value) VALUES (:id, :value);", params));

        // Read it back through RemoteSQLite with a param-bound WHERE — exercises the read forwarding
        // leg too, not just the write leg.
        SQResult result;
        ASSERT_TRUE(remote.read("SELECT id, value FROM test WHERE id = :id;",
                                {{":id", SQliteParameter::i(id)}}, result));
        ASSERT_EQUAL(result.size(), (size_t) 1);
        ASSERT_EQUAL(result[0]["id"], to_string(id));
        ASSERT_EQUAL(result[0]["value"], value);
    }
} __BoundParametersTest;
