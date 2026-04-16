#include <fstream>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/clustertest/BedrockClusterTester.h>

struct CompressionTest : tpunit::TestFixture
{
    CompressionTest()
        : tpunit::TestFixture("Compression",
                              BEFORE_CLASS(CompressionTest::setup),
                              AFTER_CLASS(CompressionTest::teardown),
                              TEST(CompressionTest::testCompressionDisabled),
                              TEST(CompressionTest::testCompressionEnabled),
                              TEST(CompressionTest::testAllNodesCompressed))
    {
    }

    BedrockClusterTester* tester;

    // Helper to read the dictionary file from test/sample_data.
    string readDictionaryFile()
    {
        string dictPath = "test/sample_data/journal.dict";
        ifstream file(dictPath, ios::binary);
        if (!file.is_open()) {
            // Try from the clustertest working directory.
            dictPath = "../sample_data/journal.dict";
            file.open(dictPath, ios::binary);
        }
        if (!file.is_open()) {
            STHROW("Could not open journal.dict");
        }
        return string((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
    }

    void setup()
    {
        // Read the dictionary file and hex-encode it for insertion via SQL.
        string dictData = readDictionaryFile();
        string hexDict;
        for (unsigned char c : dictData) {
            char buf[3];
            snprintf(buf, sizeof(buf), "%02x", c);
            hexDict += buf;
        }

        // Create the zstdDictionaries table and insert our test dictionary.
        // The table is normally created by the Zstd plugin's upgradeDatabase, but we insert the data here
        // so it's available before the cluster starts.
        list<string> queries = {
            "CREATE TABLE IF NOT EXISTS zstdDictionaries (dictionaryID INTEGER PRIMARY KEY, description TEXT, dictionary BLOB);",
            "INSERT INTO zstdDictionaries (dictionaryID, description, dictionary) VALUES (1, 'journal test dictionary', X'" + hexDict + "');",
        };

        // Start a 3-node cluster without -journalZstdDictionaryID (compression disabled).
        tester = new BedrockClusterTester(ClusterSize::THREE_NODE_CLUSTER, queries);
    }

    void teardown()
    {
        delete tester;
    }

    // Generate a long query string (~10KB) of repeated INSERT statements.
    // startID offsets the IDs so different tests don't overwrite each other's data.
    string generateLongQuery(int startID = 0)
    {
        string query;
        for (int i = 0; i < 200; i++) {
            if (!query.empty()) {
                query += ";";
            }
            query += "INSERT INTO test VALUES(" + SQ(startID + i) + ", " + SQ("value_" + to_string(startID + i) + "_padding_data_to_make_this_longer_" + string(20, 'x')) + ")";
        }
        query += ";";
        return query;
    }

    // Run a query via the DB plugin and return the result as an SQResult.
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

    // Get the current commit count from a node via the Status command.
    uint64_t getCommitCount(BedrockTester& node)
    {
        SData status("Status");
        string response = node.executeWaitVerifyContent(status);
        STable json = SParseJSONObject(response);
        return SToUInt64(json["commitCount"]);
    }

    // Run a SELECT query against the journal tables via the bedrock server's DB plugin.
    // All reads go through the server so that UDFs and dictionaries are available.
    string queryJournal(BedrockTester& node, const string& selectExpr, uint64_t commitID)
    {
        // Build a UNION query across all journal tables.
        // First, get the list of journal table names from the server.
        SData tableCmd("Query");
        tableCmd["Format"] = "json";
        tableCmd["Query"] = "SELECT tbl_name FROM sqlite_master WHERE tbl_name LIKE 'journal%' ORDER BY tbl_name;";
        auto tableResults = node.executeWaitMultipleData({tableCmd});
        SQResult tables;
        tables.deserialize(tableResults[0].content);

        // Build UNION query across all journal tables.
        string sql;
        for (size_t i = 0; i < tables.size(); i++) {
            if (!sql.empty()) {
                sql += " UNION ";
            }
            sql += "SELECT " + selectExpr + " FROM " + tables[i][0] + " WHERE id = " + SQ(commitID);
        }
        sql += ";";

        SData command("Query");
        command["Format"] = "json";
        command["Query"] = sql;
        auto results = node.executeWaitMultipleData({command});
        if (results[0].methodLine == "200 OK" && !results[0].content.empty()) {
            SQResult result;
            result.deserialize(results[0].content);
            if (!result.empty()) {
                return result[0][0];
            }
        }
        return "";
    }

    // Get the byte length of the raw query column (may be compressed binary).
    // We use LENGTH(CAST(...AS BLOB)) because compressed data is binary and can't round-trip through JSON.
    size_t readRawJournalEntrySize(BedrockTester& node, uint64_t commitID)
    {
        string result = queryJournal(node, "LENGTH(CAST(query AS BLOB))", commitID);
        return result.empty() ? 0 : SToUInt64(result);
    }

    // Read the query column with decompression applied (returns text, safe for JSON).
    string readDecompressedJournalEntry(BedrockTester& node, uint64_t commitID)
    {
        return queryJournal(node, "decompress(query)", commitID);
    }

    void testCompressionDisabled()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);

        // Stop node 2 before we write. It will miss this uncompressed commit and will need to
        // sync it later when compression is enabled, verifying that sync works with mixed data.
        tester->stopNode(2);

        // Record the commit count before our write.
        uint64_t commitBefore = getCommitCount(leader);

        // Generate and execute a long query on the leader. Use startID=0 for test 1.
        string longQuery = generateLongQuery(0);
        SData command("Query");
        command["Query"] = longQuery;
        leader.executeWaitVerifyContent(command, "200");

        // The new commit should be commitBefore + 1.
        uint64_t commitAfter = getCommitCount(leader);
        ASSERT_EQUAL(commitBefore + 1, commitAfter);

        // Read the journal entry by ID on the leader and verify it matches.
        // With compression disabled, raw size should equal the original and decompressed content should match.
        size_t leaderRawSize = readRawJournalEntrySize(leader, commitAfter);
        ASSERT_EQUAL(leaderRawSize, longQuery.size());
        string leaderDecompressed = readDecompressedJournalEntry(leader, commitAfter);
        ASSERT_EQUAL(leaderDecompressed, longQuery);

        // Wait for the follower to replicate, then verify its journal entry matches too.
        follower.waitForStatusTerm("commitCount", to_string(commitAfter));
        size_t followerRawSize = readRawJournalEntrySize(follower, commitAfter);
        ASSERT_EQUAL(followerRawSize, longQuery.size());
        string followerDecompressed = readDecompressedJournalEntry(follower, commitAfter);
        ASSERT_EQUAL(followerDecompressed, longQuery);
    }

    void testCompressionEnabled()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);

        // Stop the leader and restart it with compression enabled.
        tester->stopNode(0);
        leader.updateArgs({{"-journalZstdDictionaryID", "1"}});
        tester->startNode(0);
        ASSERT_TRUE(leader.waitForState("LEADING"));

        // Record the commit count before our write.
        uint64_t commitBefore = getCommitCount(leader);

        // Generate and execute a long query on the leader. Use startID=1000 for test 2 to avoid collisions.
        string longQuery = generateLongQuery(1000);
        SData command("Query");
        command["Query"] = longQuery;
        leader.executeWaitVerifyContent(command, "200");

        // The new commit should be commitBefore + 1.
        uint64_t commitAfter = getCommitCount(leader);
        ASSERT_EQUAL(commitBefore + 1, commitAfter);

        // Read the raw journal entry size on the leader. It should be compressed and thus shorter than the original.
        size_t leaderRawSize = readRawJournalEntrySize(leader, commitAfter);
        ASSERT_GREATER_THAN(leaderRawSize, (size_t) 0);
        ASSERT_LESS_THAN(leaderRawSize, longQuery.size());

        // Read with decompress() and verify the content matches the original query.
        string leaderDecompressed = readDecompressedJournalEntry(leader, commitAfter);
        ASSERT_EQUAL(leaderDecompressed, longQuery);

        // Wait for the follower to replicate.
        follower.waitForStatusTerm("commitCount", to_string(commitAfter));

        // The follower does not have compression enabled, so its journal stores uncompressed data.
        // Verify the follower's decompressed journal entry still matches (decompress is a no-op on uncompressed data).
        string followerDecompressed = readDecompressedJournalEntry(follower, commitAfter);
        ASSERT_EQUAL(followerDecompressed, longQuery);
    }

    void testAllNodesCompressed()
    {
        // Stop all remaining nodes (node 0 is running with compression, node 1 without, node 2 was stopped in test 1).
        tester->stopNode(0);
        tester->stopNode(1);

        // Restart all three nodes with compression enabled.
        // Use startNodeDontWait to avoid blocking — a node can't open its command port
        // until it has quorum, which requires its peers to be running.
        for (int i = 0; i < 3; i++) {
            tester->getTester(i).updateArgs({{"-journalZstdDictionaryID", "1"}});
            tester->startNodeDontWait(i);
        }
        ASSERT_TRUE(tester->getTester(0).waitForState("LEADING"));
        ASSERT_TRUE(tester->getTester(1).waitForState("FOLLOWING"));
        ASSERT_TRUE(tester->getTester(2).waitForState("FOLLOWING"));

        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower1 = tester->getTester(1);
        BedrockTester& follower2 = tester->getTester(2);

        // Record the commit count before our write.
        uint64_t commitBefore = getCommitCount(leader);

        // Insert a simple, verifiable row. Use ID 9999 to avoid collision with earlier tests.
        SData command("Query");
        command["Query"] = "INSERT INTO test VALUES(9999, 'Verifying test 3');";
        leader.executeWaitVerifyContent(command, "200");

        uint64_t commitAfter = getCommitCount(leader);
        ASSERT_EQUAL(commitBefore + 1, commitAfter);

        // Wait for both followers to replicate.
        follower1.waitForStatusTerm("commitCount", to_string(commitAfter));
        follower2.waitForStatusTerm("commitCount", to_string(commitAfter));

        // Verify the journal entry is compressed on all three nodes.
        string originalQuery = "INSERT INTO test VALUES(9999, 'Verifying test 3');";
        for (int i = 0; i < 3; i++) {
            BedrockTester& node = tester->getTester(i);
            size_t rawSize = readRawJournalEntrySize(node, commitAfter);
            ASSERT_GREATER_THAN(rawSize, (size_t) 0);
            ASSERT_LESS_THAN(rawSize, originalQuery.size());

            string decompressed = readDecompressedJournalEntry(node, commitAfter);
            ASSERT_EQUAL(decompressed, originalQuery);
        }

        // Verify the actual DB content on all three nodes.
        for (int i = 0; i < 3; i++) {
            BedrockTester& node = tester->getTester(i);
            SQResult result = queryServer(node, "SELECT id, value FROM test WHERE id = 9999;");
            ASSERT_EQUAL(result.size(), (size_t) 1);
            ASSERT_EQUAL(result[0][0], "9999");
            ASSERT_EQUAL(result[0][1], "Verifying test 3");
        }

        // Sanity check: verify total row count in the test table across all tests.
        // Test 1 inserted 200 rows (IDs 0-199), test 2 inserted 200 rows (IDs 1000-1199), test 3 inserted 1 row (ID 9999).
        for (int i = 0; i < 3; i++) {
            BedrockTester& node = tester->getTester(i);
            SQResult result = queryServer(node, "SELECT COUNT(*) FROM test;");
            ASSERT_EQUAL(result[0][0], "401");
        }
    }
} __CompressionTest;
