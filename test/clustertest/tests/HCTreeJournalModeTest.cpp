#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLite.h>
#include <libstuff/sqlite3hct.h>
#include <sys/wait.h>
#include <test/clustertest/BedrockClusterTester.h>

struct HCTreeJournalModeTest : tpunit::TestFixture
{
    HCTreeJournalModeTest()
        : tpunit::TestFixture("HCTreeJournalMode",
                              TEST(HCTreeJournalModeTest::leaderFailover),
                              TEST(HCTreeJournalModeTest::initializationPreservesHistory),
                              TEST(HCTreeJournalModeTest::rejectsDowngrade),
                              TEST(HCTreeJournalModeTest::rejectsIncompleteInitialization),
                              TEST(HCTreeJournalModeTest::rejectsJournalGap),
                              TEST(HCTreeJournalModeTest::rejectsJournalOverlap),
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
        for (BedrockTester* node : {&first, &second, &third}) {
            EXPECT_EQUAL(node->readDB("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'journal%';"), "0");
        }

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
        EXPECT_EQUAL(first.readDB("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'journal%';"), "0");
    }

    void createLegacyHistory(BedrockTester& node, SQResult& history)
    {
        // The test process uses legacy commits; the server enables experimental HC-Tree on startup.
        ASSERT_FALSE(SQLite::hctreeExperimentalMode);
        SQLite legacy(node.getArg("-db"), 1000, 25000, 1, 0, true);
        ASSERT_TRUE(legacy.beginTransaction());
        ASSERT_TRUE(legacy.write("CREATE TABLE legacyData(id INTEGER PRIMARY KEY, value TEXT);"));
        ASSERT_TRUE(legacy.prepare());
        ASSERT_EQUAL(legacy.commit(), SQLITE_OK);
        for (int i = 0; i < 20; ++i) {
            ASSERT_TRUE(legacy.beginTransaction());
            ASSERT_TRUE(legacy.write("INSERT INTO legacyData VALUES (" + SQ(i) + ", 'preserve this history');"));
            ASSERT_TRUE(legacy.prepare());
            ASSERT_EQUAL(legacy.commit(), SQLITE_OK);
        }
        ASSERT_TRUE(legacy.read("SELECT id, hex(query) AS query, hash FROM journalEntries ORDER BY id;", history));
    }

    void initializationPreservesHistory()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }

        BedrockTester node({{"-journalTables", "4"}, {"-journalDeleterBatchSize", "0"}}, {}, 0, 0, 0, false);
        SQResult history;
        createLegacyHistory(node, history);
        ASSERT_FALSE(history.empty());
        const uint64_t lastLegacyID = SToUInt64(history[history.size() - 1]["id"]);
        SQResult hctHistory;
        uint64_t lastHCTreeID = 0;

        // Promotion and a subsequent restart must preserve every commit, including the moved baseline's bytes.
        for (int restart = 0; restart < 2; ++restart) {
            node.startServer();
            ASSERT_TRUE(node.waitForState("LEADING"));
            EXPECT_EQUAL(node.readDB("SELECT MIN(cid) FROM hct_journal;"), to_string(lastLegacyID));
            EXPECT_EQUAL(node.readDB("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'journal%';"), "3");
            EXPECT_EQUAL(node.readDB("SELECT COUNT(*) FROM legacyData;"), restart ? "23" : "20");

            SQResult retained;
            ASSERT_TRUE(node.readDB("SELECT id, hex(query) AS query, hash FROM journalEntries WHERE id <= " + SQ(lastLegacyID) + " ORDER BY id;", retained));
            EXPECT_EQUAL(retained.serializeToText(), history.serializeToText());
            EXPECT_EQUAL(node.readDB("SELECT MAX(id) FROM (SELECT id FROM journal UNION ALL SELECT id FROM journal0000 UNION ALL SELECT id FROM journal0001);"),
                         to_string(lastLegacyID - 1));
            verifyHCTreeCommit(node, lastLegacyID);
            if (!restart) {
                for (int i = 20; i < 23; ++i) {
                    SData write("Query");
                    write["Query"] = "INSERT INTO legacyData VALUES (" + SQ(i) + ", 'after initialization');";
                    node.executeWaitVerifyContent(write);
                }
                lastHCTreeID = commitCount(node);
                ASSERT_GREATER_THAN(lastHCTreeID, lastLegacyID);
                ASSERT_TRUE(node.readDB("SELECT id, hex(query) AS query, hash FROM journalEntries ORDER BY id;", hctHistory));
            } else {
                SQResult afterRestart;
                ASSERT_TRUE(node.readDB("SELECT id, hex(query) AS query, hash FROM journalEntries WHERE id <= " + SQ(lastHCTreeID) + " ORDER BY id;", afterRestart));
                EXPECT_EQUAL(afterRestart.serializeToText(), hctHistory.serializeToText());
            }
            node.stopServer();
        }
    }

    bool readStoppedDB(BedrockTester& node, const string& query, SQResult& result)
    {
        sqlite3* handle = nullptr;
        const int rc = sqlite3_open_v2(node.getArg("-db").c_str(), &handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nullptr);
        unique_ptr<sqlite3, decltype(& sqlite3_close)> db(handle, sqlite3_close);
        return rc == SQLITE_OK && SQuery(db.get(), query, result) == SQLITE_OK;
    }

    void verifyStartupRejected(BedrockTester& node, bool experimental)
    {
        const vector<string> queries = {
            "SELECT name, sql FROM sqlite_schema ORDER BY name;",
            "SELECT id, hex(query) AS query, hash FROM journalEntries ORDER BY id;",
            "SELECT cid, hex(query) AS query, snapshot FROM hct_journal ORDER BY cid;",
        };
        vector<string> before;
        for (const string& query : queries) {
            SQResult result;
            ASSERT_TRUE(readStoppedDB(node, query, result));
            before.push_back(result.serializeToText());
        }

        // This server must fail before opening its ports, so do not use startServer's readiness wait.
        vector<string> args = {node.serverName, "-plugins", "db", "-journalTables", "4", "-mmapSizeGB", "1"};
        for (const string arg : {"-db", "-serverHost", "-nodeHost", "-controlPort", "-commandPortPrivate"}) {
            args.push_back(arg);
            args.push_back(node.getArg(arg));
        }
        if (experimental) {
            args.push_back("-hctreeExperimentalMode");
        }
        vector<char*> argv;
        for (string& arg : args) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);
        const pid_t child = fork();
        ASSERT_GREATER_THAN_EQUAL(child, 0);
        if (!child) {
            execvp(argv[0], argv.data());
            _exit(127);
        }

        int status = 0;
        pid_t exited = 0;
        const uint64_t deadline = STimeNow() + 15'000'000;
        while (!exited && STimeNow() < deadline) {
            exited = waitpid(child, &status, WNOHANG);
            if (!exited) {
                usleep(50'000);
            }
        }
        if (!exited) {
            kill(child, SIGKILL);
            waitpid(child, &status, 0);
        }
        ASSERT_EQUAL(exited, child);
        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQUAL(WTERMSIG(status), SIGABRT);

        for (size_t i = 0; i < queries.size(); ++i) {
            SQResult result;
            ASSERT_TRUE(readStoppedDB(node, queries[i], result));
            EXPECT_EQUAL(result.serializeToText(), before[i]);
        }
    }

    void rejectsDowngrade()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }
        BedrockTester node({{"-journalDeleterBatchSize", "0"}}, {});
        ASSERT_TRUE(node.waitForState("LEADING"));
        SData write("Query");
        write["Query"] = "CREATE TABLE downgradeTest(value INTEGER);";
        node.executeWaitVerifyContent(write);
        write["Query"] = "INSERT INTO downgradeTest VALUES(123);";
        node.executeWaitVerifyContent(write);
        const uint64_t cid = commitCount(node);
        node.stopServer();
        verifyStartupRejected(node, false);

        node.startServer();
        ASSERT_TRUE(node.waitForState("LEADING"));
        EXPECT_EQUAL(node.readDB("SELECT value FROM downgradeTest;"), "123");
        verifyHCTreeCommit(node, cid);
        EXPECT_EQUAL(node.readDB("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'journal%';"), "0");
    }

    void rejectsIncompleteInitialization()
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }
        BedrockTester node({}, {}, 0, 0, 0, false);
        SQResult history;
        createLegacyHistory(node, history);
        ASSERT_FALSE(history.empty());
        {
            sqlite3* handle = nullptr;
            ASSERT_EQUAL(sqlite3_open(node.getArg("-db").c_str(), &handle), SQLITE_OK);
            unique_ptr<sqlite3, decltype(& sqlite3_close)> db(handle, sqlite3_close);
            // SQLite commits journal creation before Bedrock can transfer the baseline and create the anchor.
            ASSERT_EQUAL(sqlite3_hct_journal_init(db.get()), SQLITE_OK);
        }
        verifyStartupRejected(node, true);
    }

    void rejectsJournalGap()
    {
        rejectJournalBoundary(false);
    }

    void rejectsJournalOverlap()
    {
        rejectJournalBoundary(true);
    }

    void rejectJournalBoundary(bool overlap)
    {
        if (!BedrockTester::ENABLE_HCTREE) {
            return;
        }

        BedrockClusterTester cluster(ClusterSize::THREE_NODE_CLUSTER, {}, {{"-journalDeleterBatchSize", "0"}});
        BedrockTester& leader = cluster.getTester(0);
        BedrockTester& follower = cluster.getTester(1);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));
        for (int i = 0; i < 20; ++i) {
            SData write("Query");
            write["Query"] = "INSERT INTO test VALUES(" + SQ(876560000 + i) + ", 'journal boundary');";
            leader.executeWaitVerifyContent(write, "200");
        }
        const uint64_t latest = commitCount(leader);
        ASSERT_TRUE(follower.waitForStatusTerm("commitCount", to_string(latest)));
        cluster.stopNode(1);

        // Split real history between two journals, leaving either one missing ID or one duplicated ID at the boundary.
        const uint64_t oldestHCTree = latest - 1;
        const uint64_t newestLegacy = overlap ? oldestHCTree : oldestHCTree - 2;
        {
            sqlite3* handle = nullptr;
            ASSERT_EQUAL(sqlite3_open(follower.getArg("-db").c_str(), &handle), SQLITE_OK);
            unique_ptr<sqlite3, decltype(& sqlite3_close)> db(handle, sqlite3_close);
            ASSERT_EQUAL(SQuery(db.get(), "CREATE TABLE journal(id INTEGER PRIMARY KEY, query TEXT, hash TEXT);"), SQLITE_OK);
            ASSERT_EQUAL(SQuery(db.get(), "INSERT INTO journal SELECT cid, substr(CAST(query AS BLOB), 75), "
                               "CAST(substr(CAST(query AS BLOB), 1, 73) AS TEXT) FROM hct_journal WHERE cid <= " + SQ(newestLegacy)), SQLITE_OK);
            ASSERT_EQUAL(SQuery(db.get(), "DELETE FROM hct_journal WHERE cid < " + SQ(oldestHCTree)), SQLITE_OK);
        }

        // The configuration is rejected at promotion, so the node must first be able to follow normally.
        cluster.startNode(1);
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));
        cluster.stopNode(0);

        int status = 0;
        pid_t exited = 0;
        const uint64_t deadline = STimeNow() + 15'000'000;
        while (!exited && STimeNow() < deadline) {
            exited = waitpid(follower.getPID(), &status, WNOHANG);
            if (!exited) {
                usleep(50'000);
            }
        }
        ASSERT_EQUAL(exited, follower.getPID());
        follower.stopServer();
        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQUAL(WTERMSIG(status), SIGABRT);

        sqlite3* handle = nullptr;
        ASSERT_EQUAL(sqlite3_open(follower.getArg("-db").c_str(), &handle), SQLITE_OK);
        unique_ptr<sqlite3, decltype(& sqlite3_close)> db(handle, sqlite3_close);
        SQResult remaining;
        ASSERT_EQUAL(SQuery(db.get(), "SELECT MIN(id) AS oldest, MAX(id) AS newest, COUNT(*) AS count FROM journal;", remaining), SQLITE_OK);
        ASSERT_EQUAL(remaining.size(), 1ul);
        EXPECT_EQUAL(remaining[0]["oldest"], "1");
        EXPECT_EQUAL(remaining[0]["newest"], to_string(newestLegacy));
        EXPECT_EQUAL(remaining[0]["count"], to_string(newestLegacy));
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
