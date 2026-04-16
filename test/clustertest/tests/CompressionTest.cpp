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
                              TEST(CompressionTest::testCompressionDisabled))
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
    string generateLongQuery()
    {
        string query;
        for (int i = 0; i < 200; i++) {
            if (!query.empty()) {
                query += ";";
            }
            query += "INSERT OR REPLACE INTO test VALUES(" + SQ(i) + ", " + SQ("value_" + to_string(i) + "_padding_data_to_make_this_longer_" + string(20, 'x')) + ")";
        }
        query += ";";
        return query;
    }

    // Get the current commit count from a node via the Status command.
    uint64_t getCommitCount(BedrockTester& node)
    {
        SData status("Status");
        string response = node.executeWaitVerifyContent(status);
        STable json = SParseJSONObject(response);
        return SToUInt64(json["commitCount"]);
    }

    // Read a journal entry by commit ID from a node's DB. Searches all journal tables.
    string readJournalEntry(BedrockTester& node, uint64_t commitID)
    {
        // Try the base journal table first.
        string result = node.readDB("SELECT query FROM journal WHERE id = " + SQ(commitID));
        if (!result.empty()) {
            return result;
        }

        // Check numbered journal tables.
        SQResult tables;
        node.readDB("SELECT tbl_name FROM sqlite_master WHERE tbl_name LIKE 'journal0%' ORDER BY tbl_name;", tables);
        for (size_t i = 0; i < tables.size(); i++) {
            result = node.readDB("SELECT query FROM " + tables[i][0] + " WHERE id = " + SQ(commitID));
            if (!result.empty()) {
                return result;
            }
        }
        return "";
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

        // Generate and execute a long query on the leader.
        string longQuery = generateLongQuery();
        SData command("Query");
        command["Query"] = longQuery;
        leader.executeWaitVerifyContent(command, "200");

        // The new commit should be commitBefore + 1.
        uint64_t commitAfter = getCommitCount(leader);
        ASSERT_EQUAL(commitBefore + 1, commitAfter);

        // Read the journal entry by ID on the leader and verify it matches.
        string leaderJournalEntry = readJournalEntry(leader, commitAfter);
        ASSERT_FALSE(leaderJournalEntry.empty());
        ASSERT_EQUAL(leaderJournalEntry, longQuery);

        // Wait for the follower to replicate, then verify its journal entry matches too.
        follower.waitForStatusTerm("commitCount", to_string(commitAfter));
        string followerJournalEntry = readJournalEntry(follower, commitAfter);
        ASSERT_FALSE(followerJournalEntry.empty());
        ASSERT_EQUAL(followerJournalEntry, longQuery);
    }
} __CompressionTest;
