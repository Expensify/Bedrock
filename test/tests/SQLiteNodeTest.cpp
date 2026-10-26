#include <libstuff/libstuff.h>
#include <libstuff/sqlite3.h>
#include <libstuff/sqlite3hct.h>
#include <plugins/Compression.h>
#include <sqlitecluster/SQLiteCommand.h>
#include <sqlitecluster/SQLiteCore.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLitePeer.h>
#include <sqlitecluster/SQLiteServer.h>
#include <test/lib/BedrockTester.h>

#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <thread>

class SQLiteNodeTester {
public:
    static SQLitePeer* getSyncPeer(SQLiteNode& node)
    {
        return node._syncPeer;
    }

    static void updateSyncPeer(SQLiteNode& node)
    {
        node._updateSyncPeer();
    }

    static void updateCommandPortForWALSize(SQLiteNode& node, uint64_t outstandingFramesToCheckpoint)
    {
        node._updateCommandPortForWALSize(outstandingFramesToCheckpoint);
    }
};

class TestServer : public SQLiteServer {
public:
    list<string> commandPortBlockReasons;
    list<string> commandPortUnblockReasons;

    TestServer() : SQLiteServer()
    {
    }

    virtual bool canStandDown()
    {
        return true;
    }

    virtual void onNodeLogin(SQLitePeer* peer)
    {
    }

    virtual void notifyStateChangeToPlugins(SQLite& db, SQLiteNodeState newState)
    {
    }

    virtual void blockCommandPort(const string& reason)
    {
        commandPortBlockReasons.push_back(reason);
    };
    virtual void unblockCommandPort(const string& reason)
    {
        commandPortUnblockReasons.push_back(reason);
    };
};

struct SQLiteNodeTest : tpunit::TestFixture
{
    SQLiteNodeTest() : tpunit::TestFixture("SQLiteNode",
                                           BEFORE_CLASS(SQLiteNodeTest::setup),
                                           AFTER_CLASS(SQLiteNodeTest::teardown),
                                           AFTER(SQLiteNodeTest::rollback),
                                           TEST(SQLiteNodeTest::testPluginDBPoolLease),
                                           TEST(SQLiteNodeTest::testFindSyncPeer),
                                           TEST(SQLiteNodeTest::testCommandPortBlockedForWALSize),
                                           TEST(SQLiteNodeTest::testGetPeerByName),
                                           TEST(SQLiteNodeTest::testAllBlankJournal),
                                           TEST(SQLiteNodeTest::testPrepareGUID),
                                           TEST(SQLiteNodeTest::testGenerateGUID),
                                           TEST(SQLiteNodeTest::testBlankHashHistory),
                                           TEST(SQLiteNodeTest::testBlankAgreement),
                                           TEST(SQLiteNodeTest::testBlankCommitFailure),
                                           TEST(SQLiteNodeTest::testGUIDHashFailures),
                                           TEST(SQLiteNodeTest::testReplicationRequiresLeader),
                                           TEST(SQLiteNodeTest::testExplicitReplicationIDs),
                                           TEST(SQLiteNodeTest::testReorderedReplication),
                                           TEST(SQLiteNodeTest::testParallelReplication),
                                           TEST(SQLiteNodeTest::testReplicationReconnect),
                                           TEST(SQLiteNodeTest::testReplicationValidation),
                                           TEST(SQLiteNodeTest::testReplicationRecovery),
                                           TEST(SQLiteNodeTest::testSynchronizeCommitFailure),
                                           TEST(SQLiteNodeTest::testSynchronizeWriteFailure),
                                           TEST(SQLiteNodeTest::testSynchronizeConstraintFailure),
                                           TEST(SQLiteNodeTest::testSynchronizeHashMismatch),
                                           TEST(SQLiteNodeTest::testPoolSharedDataReset))
    {
    }

    // Filename for temp DB.
    char filename[17] = "br_sync_dbXXXXXX";

    TestServer server;
    atomic<int> configuredPriority{1};
    atomic<uint64_t> maxOutstandingWALFrames{0};
    string peerList = "host1.fake:15555?nodeName=peer1,host2.fake:16666?nodeName=peer2,host3.fake:17777?nodeName=peer3,host4.fake:18888?nodeName=peer4";
    shared_ptr<SQLitePool> dbPool;

    void setup()
    {
        // This exposes just enough to test the peer selection logic.
        int fd = mkstemp(filename);
        close(fd);
        dbPool = make_shared<SQLitePool>(32, filename, 1000000, 5000, 0, 0, BedrockTester::ENABLE_HCTREE);
    }

    void teardown()
    {
        dbPool.reset();
        for (const auto& suffix : databaseSuffixes()) {
            unlink((string(filename) + suffix).c_str());
        }
    }

    vector<string> databaseSuffixes()
    {
        vector<string> suffixes{"", "-pagemap", "-wal", "-wal2", "-shm"};
        for (const auto& entry : filesystem::directory_iterator(".")) {
            const string name = entry.path().filename().string();
            if (SStartsWith(name, string(filename) + "-log-")) {
                suffixes.push_back(name.substr(strlen(filename)));
            }
        }
        return suffixes;
    }

    void rollback()
    {
        // Keep a failed assertion from leaving the fixture's shared handle locked for the next test.
        dbPool->getBase().rollback();
        dbPool->getBase().setCommitEnabled(true);
        maxOutstandingWALFrames = 0;
    }

    void restartDB()
    {
        dbPool.reset();
        dbPool = make_shared<SQLitePool>(32, filename, 1000000, 5000, 0, 0, BedrockTester::ENABLE_HCTREE);
    }

    void testPoolSharedDataReset()
    {
        const auto before = dbPool->getBase().getCommitState();
        {
            SQLite& db = dbPool->getBase();
            ASSERT_TRUE(db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE));
            ASSERT_TRUE(db.writeUnmodified("CREATE TABLE sharedDataRollback (id INTEGER);"));
            db.setCommitEnabled(false);
        }
        restartDB();

        SQLite& db = dbPool->getBase();
        const auto after = db.getCommitState();
        EXPECT_EQUAL(after.commitCount, before.commitCount);
        EXPECT_EQUAL(after.hashCommitID, before.hashCommitID);
        EXPECT_EQUAL(after.hash, before.hash);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM sqlite_master WHERE name = 'sharedDataRollback';"), "0");
        EXPECT_TRUE(db.popCommittedTransactions().empty());

        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified("CREATE TABLE sharedDataReset (id INTEGER);"));
        ASSERT_TRUE(db.prepare());
        EXPECT_EQUAL(db.commit(), SQLITE_OK);
    }

    void testPluginDBPoolLease()
    {
        SQLite* pluginHandle;
        {
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            pluginHandle = &node.pluginDB.db();
            EXPECT_TRUE(pluginHandle != &dbPool->getBase());

            SQLiteScopedHandle workerHandle(*dbPool, dbPool->getIndex());
            EXPECT_TRUE(&workerHandle.db() != pluginHandle);
        }

        SQLiteScopedHandle returnedHandle(*dbPool, dbPool->getIndex());
        EXPECT_TRUE(&returnedHandle.db() == pluginHandle);
    }

    void testFindSyncPeer()
    {
        SQLiteNode testNode(server, dbPool, "test", "localhost:19998", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");

        // Do a base test, with one peer with no latency.
        SQLitePeer* fastest = nullptr;
        for (auto peer : testNode._peerList) {
            int peerNum = peer->name[4] - 48;
            peer->loggedIn = true;
            peer->setCommit(10000000 + peerNum, "", 10000000 + peerNum);

            // 0, 100, 200, 300.
            peer->latency = (peerNum - 1) * 100;

            // Our fastest should be `peer2`, it has lowest non-zero latency.
            if (peer->name == "peer2") {
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);

        // See what happens when another peer becomes faster.
        for (auto peer : testNode._peerList) {
            // New fastest is peer 3.
            if (peer->name == "peer3") {
                peer->latency = 50;
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);

        // And see what happens if our fastest peer logs out.
        for (auto peer : testNode._peerList) {
            if (peer->name == "peer3") {
                peer->loggedIn = false;
                peer->latency = 50;
            }

            // 2 is fastest again.
            if (peer->name == "peer2") {
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);

        // And then if our previously 0 latency peer gets (fast) latency data.
        for (auto peer : testNode._peerList) {
            // New fastest is peer 3.
            if (peer->name == "peer1") {
                peer->latency = 75;
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);

        // Now none of our peers have latency data, but one has more commits.
        for (auto peer : testNode._peerList) {
            peer->latency = 0;

            // 4 had highest commit count.
            if (peer->name == "peer4") {
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);
    }

    void testCommandPortBlockedForWALSize()
    {
        TestServer testServer;
        SQLiteNode testNode(testServer, dbPool, "test", "localhost:19998", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");

        SQLiteNodeTester::updateCommandPortForWALSize(testNode, 300'000);
        ASSERT_TRUE(testServer.commandPortBlockReasons.empty());

        maxOutstandingWALFrames = 100'000;
        SQLiteNodeTester::updateCommandPortForWALSize(testNode, 150'000);
        SQLiteNodeTester::updateCommandPortForWALSize(testNode, 120'000);
        ASSERT_EQUAL(testServer.commandPortBlockReasons, list<string>{"WAL_TOO_LARGE"});

        SQLiteNodeTester::updateCommandPortForWALSize(testNode, 100'000);
        ASSERT_TRUE(testServer.commandPortUnblockReasons.empty());

        maxOutstandingWALFrames = 0;
        SQLiteNodeTester::updateCommandPortForWALSize(testNode, 150'000);
        SQLiteNodeTester::updateCommandPortForWALSize(testNode, 100'000);
        ASSERT_EQUAL(testServer.commandPortUnblockReasons, list<string>{"WAL_TOO_LARGE"});
    }

    void testGetPeerByName()
    {
        {
            SQLiteNode testNode(server, dbPool, "test", "localhost:19998", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            ASSERT_EQUAL(testNode.getPeerByName("peer3")->name, "peer3");
            ASSERT_EQUAL(testNode.getPeerByName("peer9"), nullptr);
        }
        {
            // It also works when the peer list isn't pre-sorted
            string unsortedPeerList = "host1.fake:15555?nodeName=peerZ,host2.fake:16666?nodeName=peer1,host3.fake:17777?nodeName=peer0,host4.fake:18888?nodeName=peerBanana";
            SQLiteNode testNode(server, dbPool, "test", "localhost:19998", unsortedPeerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            ASSERT_EQUAL(testNode.getPeerByName("peer1")->name, "peer1");
            ASSERT_EQUAL(testNode.getPeerByName("peerBanana")->name, "peerBanana");
            ASSERT_EQUAL(testNode.getPeerByName("peer9"), nullptr);
        }
    }

    void testSynchronizeCommitFailure()
    {
        testReplicationFailure("commit");
    }

    void testSynchronizeWriteFailure()
    {
        testReplicationFailure("write");
    }

    void testSynchronizeHashMismatch()
    {
        testReplicationFailure("hash");
    }

    void testSynchronizeConstraintFailure()
    {
        testReplicationFailure("constraint");
    }

    void testAllBlankJournal()
    {
        {
            SQLite& db = dbPool->getBase();
            ASSERT_EQUAL(db.getCommitCount(), 1ull);
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            SQLitePeer* peer = node.getPeerByName("peer1");
            node._changeState(SQLiteNodeState::SYNCHRONIZING);
            SData response("SYNCHRONIZE_RESPONSE");
            response["NumCommits"] = "2";
            for (int i = 2; i <= 3; ++i) {
                SData commit("COMMIT");
                commit["CommitIndex"] = to_string(i);
                commit["Hash"] = "";
                response.content += commit.serialize();
            }
            node._recvSynchronize(peer, response);
            EXPECT_EQUAL(db.getCommitCount(), 3ull);
            EXPECT_EQUAL(db.getCommitState().hashCommitID, 0ull);
            EXPECT_TRUE(db.getCommittedHash().empty());
            peer->setCommit(2, "", 0);
            SData outgoing("SYNCHRONIZE_RESPONSE");
            node._queueSynchronize(&node, peer, db, outgoing, true);
            EXPECT_EQUAL(outgoing["NumCommits"], "1");
            EXPECT_FALSE(outgoing.isSet("hashMismatchNumber"));
            peer->setCommit(2, "nonblank hash", 2);
            SData mismatch("SYNCHRONIZE_RESPONSE");
            node._queueSynchronize(&node, peer, db, mismatch, true);
            EXPECT_TRUE(mismatch.isSet("hashMismatchNumber"));
        }
        restartDB();
        EXPECT_EQUAL(dbPool->getBase().getCommitCount(), 3ull);
        EXPECT_EQUAL(dbPool->getBase().getCommitState().hashCommitID, 0ull);
        EXPECT_TRUE(dbPool->getBase().getCommittedHash().empty());
    }

    void testBlankAgreement()
    {
        SQLite& db = dbPool->getBase();
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified("CREATE TABLE IF NOT EXISTS agreementTest (id INTEGER);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        const auto anchor = db.getCommitState();
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.prepare(nullptr, nullptr, chrono::hours(24), nullptr, ""));
            ASSERT_EQUAL(db.commit(), SQLITE_OK);
        }
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
        SQLitePeer* peer = node.getPeerByName("peer1");
        const SData headers = node._addPeerHeaders(SData("STATE"));
        EXPECT_EQUAL(headers.calcU64("CommitCount"), anchor.commitCount + 3);
        EXPECT_EQUAL(headers.calcU64("HashCommitID"), anchor.hashCommitID);
        EXPECT_EQUAL(headers["Hash"], anchor.hash);

        for (uint64_t blanks = 0; blanks <= 3; ++blanks) {
            peer->reset();
            SData login("LOGIN");
            login["CommitCount"] = to_string(anchor.commitCount + blanks);
            login["Hash"] = anchor.hash;
            login["Priority"] = "2";
            login["State"] = "SEARCHING";
            login["Version"] = "1.0";
            // Exercise rollout fallback when the peer's latest entry is nonblank.
            if (blanks) {
                login["HashCommitID"] = to_string(anchor.hashCommitID);
            }
            node._onMESSAGE(peer, login);
            EXPECT_TRUE(peer->loggedIn);
            EXPECT_FALSE(peer->forked);
            uint64_t count, hashID;
            string hash;
            peer->getCommit(count, hash, hashID);
            EXPECT_EQUAL(count, anchor.commitCount + blanks);
            EXPECT_EQUAL(hashID, anchor.hashCommitID);
            EXPECT_EQUAL(hash, anchor.hash);
            SData response("SYNCHRONIZE_RESPONSE");
            node._queueSynchronize(&node, peer, db, response, true);
            EXPECT_FALSE(response.isSet("hashMismatchNumber"));
            EXPECT_EQUAL(response.calcU64("NumCommits"), 3 - blanks);
            string content = response.content;
            for (uint64_t i = blanks + 1; i <= 3; ++i) {
                SData commit;
                const size_t size = commit.deserialize(content);
                ASSERT_TRUE(size > 0);
                content.erase(0, size);
                EXPECT_EQUAL(commit.calcU64("CommitIndex"), anchor.commitCount + i);
                EXPECT_EQUAL(commit.calcU64("Snapshot"), anchor.commitCount + i - 1);
                EXPECT_TRUE(commit.isSet("Hash"));
                EXPECT_TRUE(commit["Hash"].empty());
                EXPECT_TRUE(commit.content.empty());
            }
            EXPECT_TRUE(content.empty());
        }

        // A newer nonblank commit does not invalidate agreement on a shorter shared prefix.
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified("INSERT INTO agreementTest VALUES (1);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        peer->setCommit(anchor.commitCount + 1, anchor.hash, anchor.hashCommitID);
        SData response("SYNCHRONIZE_RESPONSE");
        node._queueSynchronize(&node, peer, db, response, true);
        EXPECT_EQUAL(response["NumCommits"], "3");
        EXPECT_FALSE(response.isSet("hashMismatchNumber"));

        // A peer cannot claim that a real transaction in the common prefix was blank, or vice versa.
        // IDs are part of the identity even if a peer advertises an identical hash at a different ID.
        const vector<SQLite::CommitState> forks = {
            {anchor.commitCount + 3, anchor.hashCommitID, "different hash"},
            {anchor.commitCount + 3, anchor.commitCount + 1, anchor.hash},
            {anchor.commitCount + 4, anchor.hashCommitID, anchor.hash},
        };
        for (const auto& fork : forks) {
            peer->reset();
            peer->setCommit(fork.commitCount, fork.hash, fork.hashCommitID);
            SData mismatch("SYNCHRONIZE_RESPONSE");
            node._queueSynchronize(&node, peer, db, mismatch, true);
            EXPECT_TRUE(mismatch.isSet("hashMismatchNumber"));
            EXPECT_FALSE(mismatch.isSet("NumCommits"));

            SData login("LOGIN");
            login["CommitCount"] = to_string(fork.commitCount);
            login["HashCommitID"] = to_string(fork.hashCommitID);
            login["Hash"] = fork.hash;
            login["Priority"] = "2";
            login["State"] = "SEARCHING";
            login["Version"] = "1.0";
            node._onMESSAGE(peer, login);
            EXPECT_TRUE(peer->forked);
            EXPECT_FALSE(peer->loggedIn);
        }

        peer->reset();
        SData ping("PING");
        ping["CommitCount"] = to_string(anchor.commitCount + 3);
        ping["HashCommitID"] = to_string(anchor.hashCommitID);
        ping["Hash"] = anchor.hash;
        node._onMESSAGE(peer, ping);
        uint64_t count, hashID;
        string hash;
        peer->getCommit(count, hash, hashID);
        EXPECT_EQUAL(count, anchor.commitCount + 3);
        EXPECT_EQUAL(hashID, anchor.hashCommitID);
        EXPECT_EQUAL(hash, anchor.hash);
    }

    void testBlankCommitFailure()
    {
        SQLite& db = dbPool->getBase();
        SQLite other(db);
        const auto before = db.getCommitState();
        for (const string mode : {"live", "sync", "subscription"}) {
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            SQLitePeer* peer = node.getPeerByName("peer1");
            const bool live = mode == "live";
            const bool subscribing = mode == "subscription";
            node._changeState(live ? SQLiteNodeState::FOLLOWING :
                subscribing ? SQLiteNodeState::SUBSCRIBING : SQLiteNodeState::SYNCHRONIZING);
            peer->loggedIn = true;
            if (live || subscribing) {
                node._leadPeer = peer;
            } else {
                node._syncPeer = peer;
            }
            db.setCommitEnabled(false);
            if (live) {
                SData transaction("TRANSACTION");
                transaction["ID"] = to_string(before.commitCount + 1);
                transaction["NewCount"] = transaction["ID"];
                transaction["NewHash"] = "";
                node._handleBeginTransaction(db, peer, transaction);
                ASSERT_TRUE(node._handlePrepareTransaction(db, peer, transaction, STimeNow()));
                EXPECT_TRUE(db.isPrepared());
                EXPECT_TRUE(node._handleCommitTransaction(db, peer, before.commitCount + 1, "") == SQLite::COMMIT_DISABLED);
                node._onReplicationError(db, peer, transaction, "commit disabled");
            } else {
                SData commit("COMMIT");
                commit["CommitIndex"] = to_string(before.commitCount + 1);
                commit["Hash"] = "";
                SData response(subscribing ? "SUBSCRIPTION_APPROVED" : "SYNCHRONIZE_RESPONSE");
                response["CommitCount"] = commit["CommitIndex"];
                response["HashCommitID"] = to_string(before.hashCommitID);
                response["Hash"] = before.hash;
                response["NumCommits"] = "1";
                response.content = commit.serialize();
                node._onMESSAGE(peer, response);
            }
            EXPECT_FALSE(db.insideTransaction());
            EXPECT_FALSE(db.isPrepared());
            EXPECT_EQUAL(db.getCommitCount(), before.commitCount);
            EXPECT_EQUAL(db.getCommitState().hashCommitID, before.hashCommitID);
            EXPECT_EQUAL(db.getCommittedHash(), before.hash);
            EXPECT_FALSE(db.getCommit(before.commitCount + 1));
            db.setCommitEnabled(true);
            bool prepared = false;
            thread waiter([&]() {
                if (other.beginTransaction()) {
                    prepared = other.prepare(nullptr, nullptr, chrono::milliseconds(250), nullptr, "");
                }
                other.rollback();
            });
            waiter.join();
            EXPECT_TRUE(prepared);
        }
    }

    void testPrepareGUID()
    {
        SQLite& db = dbPool->getBase();
        const uint64_t commitCount = db.getCommitCount();
        const string committedHash = db.getCommittedHash();
        const string query = "CREATE TABLE prepareGUIDTest (id INTEGER);";
        for (const string guid : {"00000000000000000000000000aBcDeF", "00000000000000000000000000abcde0"}) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.writeUnmodified(query));
            string hash;
            ASSERT_TRUE(db.prepare(nullptr, &hash, chrono::hours(24), nullptr, guid));
            const string expectedHash = guid + ":" + SToHex(SHashSHA1(guid + query));
            EXPECT_EQUAL(hash, expectedHash);
            EXPECT_EQUAL(db.getUncommittedHash(), expectedHash);
            EXPECT_EQUAL(db.getCommittedHash(), committedHash);
            EXPECT_EQUAL(db.getCommitCount(), commitCount);
            db.rollback();
            EXPECT_TRUE(db.getUncommittedHash().empty());
        }
        for (const string& guid : {string(), string(31, '0'), string(33, '0'), string(31, '0') + "g", string(32, '0') + ":" + string(40, '0')}) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.writeUnmodified(query));
            EXPECT_FALSE(db.prepare(nullptr, nullptr, chrono::hours(24), nullptr, guid));
            EXPECT_TRUE(db.getUncommittedHash().empty());
            db.rollback();
        }
    }

    void testGenerateGUID()
    {
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
        node._changeState(SQLiteNodeState::LEADING);
        SQLite& db = dbPool->getBase();
        SQLiteCore core(db);
        set<string> guids;
        for (const string query : {"CREATE TABLE IF NOT EXISTS generatedGUIDTest (id INTEGER);", ""}) {
            // Retry identical SQL after rollback, then commit it through the local leader path.
            for (int attempt = 0; attempt < 3; ++attempt) {
                const uint64_t commitCount = db.getCommitCount();
                const string committedHash = db.getCommittedHash();
                ASSERT_TRUE(db.beginTransaction());
                ASSERT_TRUE(db.writeUnmodified(query));
                uint64_t commitID;
                string hash;
                if (attempt == 2) {
                    ASSERT_TRUE(core.commit(node, commitID, hash, "testGenerateGUID", false) == SQLiteCore::COMMIT_RESULT::OK);
                    if (db.isExperimentalHCTree() && query.empty()) {
                        EXPECT_EQUAL(commitID, 0ull);
                        EXPECT_TRUE(hash.empty());
                        EXPECT_EQUAL(db.getCommitCount(), commitCount);
                        continue;
                    }
                } else {
                    ASSERT_TRUE(db.prepare(&commitID, &hash));
                }
                ASSERT_EQUAL(hash.size(), (size_t) 73);
                EXPECT_EQUAL(hash[32], ':');
                const string guid = hash.substr(0, 32);
                EXPECT_EQUAL(guid.find_first_not_of("0123456789ABCDEFabcdef"), string::npos);
                EXPECT_TRUE(guids.insert(guid).second);
                EXPECT_EQUAL(hash, guid + ":" + SToHex(SHashSHA1(guid + query)));
                EXPECT_EQUAL(commitID, commitCount + 1);
                if (attempt == 2) {
                    EXPECT_EQUAL(db.getCommittedHash(), hash);
                    string storedQuery, storedHash;
                    ASSERT_TRUE(db.getCommit(commitID, &storedQuery, &storedHash));
                    EXPECT_EQUAL(storedQuery, query);
                    EXPECT_EQUAL(storedHash, hash);
                } else {
                    EXPECT_EQUAL(db.getUncommittedHash(), hash);
                    EXPECT_EQUAL(db.getCommittedHash(), committedHash);
                    EXPECT_EQUAL(db.getCommitCount(), commitCount);
                    db.rollback();
                }
                EXPECT_TRUE(db.getUncommittedHash().empty());
            }
        }
    }

    void testBlankHashHistory()
    {
        const vector<string> queries = {
            "",
            "CREATE TABLE IF NOT EXISTS hashTest (value INTEGER);DELETE FROM hashTest;",
            "INSERT INTO hashTest VALUES (1);",
            "",
            "INSERT INTO hashTest VALUES (1);",
            "INSERT INTO hashTest VALUES (2);",
            "",
            "",
            "",
        };
        const vector<string> guids = {
            "", "00000000000000000000000000aBcDeF", "00000000000000000000000000abcde0", "",
            "00000000000000000000000000000001", "00000000000000000000000000000002",
            "00000000000000000000000000000003", "", "",
        };
        for (const string mode : {"live", "sync", "subscription"}) {
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            SQLite& db = dbPool->getBase();
            SQLitePeer* peer = node.getPeerByName("peer1");
            const uint64_t commitCount = db.getCommitCount();
            auto expectedState = db.getCommitState();
            vector<string> hashes;
            const bool live = mode == "live";
            const bool subscribing = mode == "subscription";
            node._changeState(live ? SQLiteNodeState::FOLLOWING :
                              subscribing ? SQLiteNodeState::SUBSCRIBING : SQLiteNodeState::SYNCHRONIZING);
            peer->loggedIn = true;
            if (live || subscribing) {
                node._leadPeer = peer;
            } else {
                node._syncPeer = peer;
            }
            SData response(subscribing ? "SUBSCRIPTION_APPROVED" : "SYNCHRONIZE_RESPONSE");
            for (size_t i = 0; i < queries.size(); ++i) {
                const string hash = guids[i].empty() ? "" :
                    guids[i] + ":" + SToHex(SHashSHA1(guids[i] + queries[i]));
                hashes.push_back(hash);
                if (!hash.empty()) {
                    expectedState.hash = hash;
                    expectedState.hashCommitID = commitCount + i + 1;
                }
                string content = queries[i];
                // Mix raw and compressed wire SQL without changing the global journal compression setting.
                if (i == 2 || i == 4 || i == 5) {
                    content.resize(ZSTD_compressBound(queries[i].size()));
                    const size_t size = ZSTD_compress(content.data(), content.size(), queries[i].data(), queries[i].size(), 3);
                    ASSERT_FALSE(ZSTD_isError(size));
                    content.resize(size);
                }
                if (live) {
                    SData transaction("TRANSACTION");
                    transaction["ID"] = to_string(commitCount + i + 1);
                    transaction["NewCount"] = transaction["ID"];
                    transaction["NewHash"] = hash;
                    transaction.content = content;
                    node._handleBeginTransaction(db, peer, transaction);
                    ASSERT_TRUE(node._handlePrepareTransaction(db, peer, transaction, STimeNow()));
                    EXPECT_TRUE(db.isPrepared());
                    EXPECT_EQUAL(db.getUncommittedHash(), hash);
                    ASSERT_EQUAL(node._handleCommitTransaction(db, peer, commitCount + i + 1, hash), SQLITE_OK);
                    EXPECT_EQUAL(db.getCommittedHash(), expectedState.hash);
                    EXPECT_EQUAL(db.getCommitState().hashCommitID, expectedState.hashCommitID);
                    EXPECT_FALSE(db.isPrepared());
                } else {
                    SData commit("COMMIT");
                    commit["CommitIndex"] = to_string(commitCount + i + 1);
                    commit["Hash"] = hash;
                    commit.content = content;
                    response.content += commit.serialize();
                }
            }
            if (!live) {
                response["CommitCount"] = to_string(commitCount + queries.size());
                response["Hash"] = expectedState.hash;
                response["HashCommitID"] = to_string(expectedState.hashCommitID);
                response["NumCommits"] = to_string(queries.size());
                node._onMESSAGE(peer, response);
            }
            EXPECT_TRUE(node.getState() == (live || subscribing ? SQLiteNodeState::FOLLOWING : SQLiteNodeState::WAITING));
            EXPECT_EQUAL(db.getCommitCount(), commitCount + queries.size());
            EXPECT_EQUAL(db.getCommittedHash(), expectedState.hash);
            EXPECT_EQUAL(db.getCommitState().hashCommitID, expectedState.hashCommitID);
            EXPECT_NOT_EQUAL(hashes[2], hashes[4]);
            EXPECT_FALSE(db.insideTransaction());
            EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM hashTest;"), "3");
            for (size_t i = 0; i < queries.size(); ++i) {
                string query, hash;
                ASSERT_TRUE(db.getCommit(commitCount + i + 1, &query, &hash));
                EXPECT_EQUAL(query, queries[i]);
                EXPECT_EQUAL(hash, hashes[i]);
            }
        }

        const uint64_t commitCount = dbPool->getBase().getCommitCount();
        const string committedHash = dbPool->getBase().getCommittedHash();
        ASSERT_EQUAL(committedHash, guids[6] + ":" + SToHex(SHashSHA1(guids[6])));
        restartDB();
        SQLite& db = dbPool->getBase();
        EXPECT_EQUAL(db.getCommitCount(), commitCount);
        EXPECT_EQUAL(db.getCommittedHash(), committedHash);
        EXPECT_EQUAL(db.getCommitState().hashCommitID, commitCount - 2);
        const string query = "INSERT INTO hashTest VALUES (3);";
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified(query));
        ASSERT_TRUE(db.prepare());
        const string hash = db.getUncommittedHash();
        ASSERT_EQUAL(hash.size(), (size_t) 73);
        const string guid = hash.substr(0, 32);
        EXPECT_EQUAL(hash, guid + ":" + SToHex(SHashSHA1(guid + query)));
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        string storedHash;
        ASSERT_TRUE(db.getCommit(commitCount + 1, nullptr, &storedHash));
        EXPECT_EQUAL(storedHash, hash);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM hashTest;"), "4");
    }

    void testGUIDHashFailures()
    {
        for (const string failure : {"guid-value", "guid-digest", "guid-query", "guid-short", "guid-long", "guid-nonhex", "guid-separator", "guid-no-separator", "legacy", "blank-query"}) {
            testReplicationFailure(failure);
        }
    }

    void testReplicationRequiresLeader()
    {
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
        SQLite& db = dbPool->getBase();
        const uint64_t commitCount = db.getCommitCount();
        const string committedHash = db.getCommittedHash();
        node._changeState(SQLiteNodeState::FOLLOWING);
        node._leadPeer = node.getPeerByName("peer1");
        SQLitePeer* otherPeer = node.getPeerByName("peer2");
        otherPeer->loggedIn = true;
        // Keep outgoing messages buffered without a transport, so an unexpected RECONNECT is observable.
        auto socket = make_unique<STCPManager::Socket>(-1, STCPManager::Socket::CONNECTED);
        ASSERT_TRUE(otherPeer->setSocket(socket.get()));
        auto* peerSocket = socket.release(); // The peer owns the socket.

        SData transaction("TRANSACTION");
        transaction["CommitCount"] = to_string(commitCount + 1);
        transaction["NewCount"] = transaction["CommitCount"];
        transaction["ID"] = transaction["CommitCount"];
        transaction.content = "CREATE TABLE nonLeaderTransaction (id INTEGER);";
        const string guid = "00000000000000000000000000000001";
        transaction["NewHash"] = guid + ":" + SToHex(SHashSHA1(guid + transaction.content));
        transaction["Hash"] = transaction["NewHash"];
        node._onMESSAGE(otherPeer, transaction);

        EXPECT_TRUE(otherPeer->connected());
        EXPECT_TRUE(peerSocket->sendBufferEmpty());
        EXPECT_TRUE(node._replicateThreads.empty());
        EXPECT_TRUE(node._replicateQueue.empty());
        EXPECT_FALSE(db.insideTransaction());
        EXPECT_EQUAL(db.getCommitCount(), commitCount);
        EXPECT_EQUAL(db.getCommittedHash(), committedHash);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM sqlite_master WHERE name = 'nonLeaderTransaction';"), "0");
    }

    void testExplicitReplicationIDs()
    {
        SQLite& db = dbPool->getBase();
        if (!db.isExperimentalHCTree()) {
            return;
        }
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified("CREATE TABLE explicitReplication (id INTEGER PRIMARY KEY);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        const auto before = db.getCommitState();
        SQLite replica(db);
        const string guid = "00000000000000000000000000000001";
        const string query = "INSERT INTO explicitReplication VALUES (2);";
        const string hash = guid + ":" + SToHex(SHashSHA1(guid + query));

        // Another thread holds commitLock, but an explicit experimental follower commit must not need it.
        ASSERT_TRUE(db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE));
        string error;
        thread writer([&]() {
            SInitialize("explicitReplication");
            try {
                if (!replica.beginTransaction() || !replica.writeUnmodified(query) ||
                    !replica.prepare(nullptr, nullptr, chrono::milliseconds(250), nullptr, guid,
                                     SQLite::ReplicationInfo{before.commitCount + 2, before.commitCount}) ||
                    replica.commit() != SQLITE_OK) {
                    error = "failed out-of-order commit";
                }
            } catch (const exception& e) {
                error = e.what();
            }
            replica.rollback();
        });
        writer.join();
        db.rollback();
        ASSERT_TRUE(error.empty());
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount);
        EXPECT_EQUAL(db.getCommittedHash(), before.hash);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM explicitReplication;"), "0");
        EXPECT_EQUAL(db.getOutOfOrderCommitHash(before.commitCount + 2).value_or("missing"), hash);
        string storedHash;
        ASSERT_TRUE(db.getCommit(before.commitCount + 2, nullptr, &storedHash));
        EXPECT_EQUAL(storedHash, hash);

        // Filling the gap with a blank entry publishes both commits without replacing the newer nonblank hash.
        ASSERT_TRUE(replica.beginTransaction());
        ASSERT_TRUE(replica.prepare(nullptr, nullptr, chrono::hours(24), nullptr, "",
                                   SQLite::ReplicationInfo{before.commitCount + 1, before.commitCount}));
        ASSERT_EQUAL(replica.commit(), SQLITE_OK);
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount + 2);
        EXPECT_EQUAL(db.getCommitState().hashCommitID, before.commitCount + 2);
        EXPECT_EQUAL(db.getCommittedHash(), hash);
        EXPECT_FALSE(db.getOutOfOrderCommitHash(before.commitCount + 2).has_value());
        EXPECT_EQUAL(db.read("SELECT id FROM explicitReplication;"), "2");
        sqlite3_int64 snapshot = 0;
        EXPECT_EQUAL(sqlite3_hct_journal_snapshot(db.getDBHandle(), &snapshot), SQLITE_OK);
        EXPECT_EQUAL(static_cast<uint64_t>(snapshot), db.getCommitCount());
    }

    SData transaction(uint64_t id, const string& query, optional<uint64_t> snapshot = nullopt)
    {
        SData message("TRANSACTION");
        message["ID"] = to_string(id);
        message["NewCount"] = message["ID"];
        const string guid = SToHex(id, 32);
        message["NewHash"] = query.empty() ? "" : guid + ":" + SToHex(SHashSHA1(guid + query));
        message["CommitCount"] = message["ID"];
        message["Hash"] = message["NewHash"];
        if (snapshot.has_value()) {
            message["Snapshot"] = to_string(snapshot.value());
        }
        message.content = query;
        return message;
    }

    void testReorderedReplication()
    {
        SQLite& db = dbPool->getBase();
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified("CREATE TABLE reorderedReplication (id INTEGER PRIMARY KEY);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        const auto before = db.getCommitState();
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
        node._changeState(SQLiteNodeState::FOLLOWING);
        SQLitePeer* peer = node.getPeerByName("peer1");
        node._leadPeer = peer;
        peer->loggedIn = true;

        const auto later = transaction(before.commitCount + 2, "INSERT INTO reorderedReplication VALUES (2);", before.commitCount);
        node._onMESSAGE(peer, later);
        // Duplicate queued, active, or completed deliveries must not apply the SQL twice.
        node._onMESSAGE(peer, later);
        bool appliedAhead = false;
        {
            unique_lock<mutex> lock(node._replicateMutex);
            if (db.isExperimentalHCTree()) {
                appliedAhead = node._replicateCV.wait_for(lock, chrono::seconds(5), [&]() {
                    return db.getOutOfOrderCommitHash(before.commitCount + 2).has_value();
                });
            } else {
                EXPECT_TRUE(node._replicateQueue.contains(before.commitCount + 2));
                EXPECT_TRUE(node._replicating.empty());
            }
        }
        EXPECT_EQUAL(appliedAhead, db.isExperimentalHCTree());
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount);
        EXPECT_EQUAL(db.getCommittedHash(), before.hash);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM reorderedReplication;"), "0");
        node._onMESSAGE(peer, later);

        // An absent Snapshot conservatively requires the preceding commit, even on an experimental follower.
        const auto last = transaction(before.commitCount + 3, "INSERT INTO reorderedReplication VALUES (3);");
        node._onMESSAGE(peer, last);
        {
            lock_guard<mutex> lock(node._replicateMutex);
            EXPECT_TRUE(node._replicateQueue.contains(before.commitCount + 3));
        }
        node._onMESSAGE(peer, transaction(before.commitCount + 1, "", before.commitCount));
        bool completed;
        {
            unique_lock<mutex> lock(node._replicateMutex);
            completed = node._replicateCV.wait_for(lock, chrono::seconds(5), [&]() {
                return node._replicateQueue.empty() && node._replicating.empty();
            });
        }
        node._onMESSAGE(peer, last);
        node._changeState(SQLiteNodeState::SEARCHING);
        EXPECT_TRUE(completed);
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount + 3);
        EXPECT_EQUAL(db.getCommittedHash(), last["NewHash"]);
        EXPECT_EQUAL(db.getCommitState().hashCommitID, before.commitCount + 3);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM reorderedReplication;"), "2");
        EXPECT_TRUE(node._replicateThreads.empty());
    }

    void testParallelReplication()
    {
        SQLite& db = dbPool->getBase();
        if (!db.isExperimentalHCTree()) {
            return;
        }
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified("CREATE TABLE parallelReplication (id INTEGER PRIMARY KEY);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);

        struct Gate
        {
            mutex sync;
            condition_variable cv;
            size_t arrivals = 0;
            bool released = false;

            static int trace(unsigned, void* context, void*, void* sql)
            {
                if (!SStartsWith(static_cast<const char*>(sql), "INSERT INTO parallelReplication")) {
                    return 0;
                }
                auto& gate = *static_cast<Gate*>(context);
                unique_lock<mutex> lock(gate.sync);
                ++gate.arrivals;
                gate.cv.notify_all();
                gate.cv.wait_for(lock, chrono::seconds(5), [&]() {
                    return gate.released;
                                                                                         });
                return 0;
            }
        };

        for (const string mode : {"live", "sync", "subscription"}) {
            const uint64_t before = db.getCommitCount();
            Gate gate;
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            vector<size_t> handles;
            for (size_t i = 0; i < 16; ++i) {
                handles.push_back(dbPool->getIndex());
                sqlite3_trace_v2(dbPool->initializeIndex(handles.back()).getDBHandle(), SQLITE_TRACE_STMT, Gate::trace, &gate);
            }
            for (const auto index : handles) {
                dbPool->returnToPool(index);
            }
            const bool live = mode == "live";
            const bool subscribing = mode == "subscription";
            node._changeState(live ? SQLiteNodeState::FOLLOWING :
                              subscribing ? SQLiteNodeState::SUBSCRIBING : SQLiteNodeState::SYNCHRONIZING);
            SQLitePeer* peer = node.getPeerByName("peer1");
            peer->loggedIn = true;
            if (live || subscribing) {
                node._leadPeer = peer;
            } else {
                node._syncPeer = peer;
            }
            SData response(subscribing ? "SUBSCRIPTION_APPROVED" : "SYNCHRONIZE_RESPONSE");
            response["NumCommits"] = "16";
            response["CommitCount"] = to_string(before + 16);
            response["HashCommitID"] = response["CommitCount"];
            vector<SData> messages;
            for (uint64_t i = 1; i <= 16; ++i) {
                messages.push_back(transaction(before + i, "INSERT INTO parallelReplication VALUES (" + to_string(before + i) + ");", before));
                SData commit("COMMIT");
                commit["CommitIndex"] = messages.back()["ID"];
                commit["Hash"] = messages.back()["NewHash"];
                commit["Snapshot"] = to_string(before);
                commit.content = messages.back().content;
                response.content += commit.serialize();
                response["Hash"] = commit["Hash"];
            }
            thread receiver([&]() {
                SInitialize("parallelReceiver");
                if (live) {
                    for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
                        node._onMESSAGE(peer, *it);
                    }
                } else {
                    node._onMESSAGE(peer, response);
                }
            });
            bool allParallel;
            {
                unique_lock<mutex> lock(gate.sync);
                allParallel = gate.cv.wait_for(lock, chrono::seconds(5), [&]() {
                    return gate.arrivals == 16;
                                                                                                             });
                gate.released = true;
            }
            gate.cv.notify_all();
            receiver.join();
            node._stopReplication();
            for (const auto index : handles) {
                sqlite3_trace_v2(dbPool->initializeIndex(index).getDBHandle(), 0, nullptr, nullptr);
            }
            EXPECT_TRUE(allParallel);
            EXPECT_EQUAL(db.getCommitCount(), before + 16);
            EXPECT_EQUAL(db.getCommittedHash(), messages.back()["NewHash"]);
            EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM parallelReplication WHERE id > " + to_string(before) + ";"), "16");
            EXPECT_TRUE(node.getState() == (live || subscribing ? SQLiteNodeState::FOLLOWING : SQLiteNodeState::WAITING));
        }
    }

    void testReplicationReconnect()
    {
        SQLite& db = dbPool->getBase();
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified("CREATE TABLE reconnectReplication (id INTEGER PRIMARY KEY);"));
        ASSERT_TRUE(db.prepare());
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        const auto before = db.getCommitState();
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
        node._changeState(SQLiteNodeState::FOLLOWING);
        SQLitePeer* peer = node.getPeerByName("peer1");
        node._leadPeer = peer;
        peer->loggedIn = true;
        const auto later = transaction(before.commitCount + 2, "INSERT INTO reconnectReplication VALUES (1);", before.commitCount);
        node._onMESSAGE(peer, later);
        if (db.isExperimentalHCTree()) {
            bool applied;
            {
                unique_lock<mutex> lock(node._replicateMutex);
                applied = node._replicateCV.wait_for(lock, chrono::seconds(5), [&]() {
                    return db.getOutOfOrderCommitHash(before.commitCount + 2).has_value();
                });
            }
            EXPECT_TRUE(applied);
        }
        // A gap must not keep shutdown waiting for work that cannot run. Completed HC-Tree commits survive reconnect.
        node._changeState(SQLiteNodeState::SEARCHING);
        EXPECT_TRUE(node._replicateThreads.empty());
        EXPECT_TRUE(node._replicateQueue.empty());
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount);
        node._changeState(SQLiteNodeState::SYNCHRONIZING);
        node._syncPeer = peer;
        peer->loggedIn = true;
        SData blank("COMMIT");
        blank["CommitIndex"] = to_string(before.commitCount + 1);
        blank["Hash"] = "";
        SData response("SYNCHRONIZE_RESPONSE");
        response["NumCommits"] = "1";
        response.content = blank.serialize();
        response["CommitCount"] = to_string(before.commitCount + (db.isExperimentalHCTree() ? 2 : 1));
        response["HashCommitID"] = to_string(db.isExperimentalHCTree() ? before.commitCount + 2 : before.hashCommitID);
        response["Hash"] = db.isExperimentalHCTree() ? later["NewHash"] : before.hash;
        node._onMESSAGE(peer, response);
        EXPECT_TRUE(node.getState() == SQLiteNodeState::WAITING);
        node._changeState(SQLiteNodeState::FOLLOWING);
        node._leadPeer = peer;
        node._onMESSAGE(peer, later);
        node._changeState(SQLiteNodeState::SEARCHING);
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount + 2);
        EXPECT_EQUAL(db.getCommittedHash(), later["NewHash"]);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM reconnectReplication;"), "1");
    }

    void testReplicationValidation()
    {
        SQLite& db = dbPool->getBase();
        const auto before = db.getCommitState();
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
        node._changeState(SQLiteNodeState::SYNCHRONIZING);
        SQLitePeer* peer = node.getPeerByName("peer1");
        const auto valid = transaction(before.commitCount + 1, "", before.commitCount);
        for (const string& value : vector<string>{"-1", "", "no", "9223372036854775808", to_string(before.commitCount + 1)}) {
            SData invalid = valid;
            invalid["Snapshot"] = value;
            bool rejected = false;
            try {
                node._enqueueReplication(peer, move(invalid));
            } catch (const exception&) {
                rejected = true;
            }
            EXPECT_TRUE(rejected);
            EXPECT_TRUE(node._replicateThreads.empty());
        }
        for (const string count : {"-1", "", "invalid", "2147483648", "1"}) {
            SData response("SYNCHRONIZE_RESPONSE");
            response["NumCommits"] = count;
            bool rejected = false;
            try {
                node._recvSynchronize(peer, response);
            } catch (const exception&) {
                rejected = true;
            }
            EXPECT_TRUE(rejected);
        }
        SData first("COMMIT");
        first["CommitIndex"] = valid["ID"];
        first["Hash"] = "";
        SData truncated("SYNCHRONIZE_RESPONSE");
        truncated["NumCommits"] = "2";
        truncated.content = first.serialize() + "COMMIT\r\nCommitIndex:";
        bool rejected = false;
        try {
            node._recvSynchronize(peer, truncated);
        } catch (const exception&) {
            rejected = true;
        }
        EXPECT_TRUE(rejected);
        EXPECT_TRUE(node._replicateThreads.empty());
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount);

        // Dependencies with a missing predecessor, and IDs outside HC-Tree's commit window, must not occupy workers.
        node._enqueueReplication(peer, transaction(before.commitCount + 2, "", before.commitCount + 1));
        node._enqueueReplication(peer, transaction(before.commitCount + SQLiteNode::MAX_REPLICATION_AHEAD + 1, "", before.commitCount));
        {
            lock_guard<mutex> lock(node._replicateMutex);
            EXPECT_EQUAL(node._replicateQueue.size(), 2ul);
            EXPECT_TRUE(node._replicating.empty());
        }
        node._changeState(SQLiteNodeState::SEARCHING);
        EXPECT_TRUE(node._replicateThreads.empty());
        EXPECT_EQUAL(db.getCommitCount(), before.commitCount);
    }

    void testReplicationRecovery()
    {
        if (!dbPool->getBase().isExperimentalHCTree()) {
            return;
        }
        uint64_t before = 0;
        SData later;
        {
            SQLite& db = dbPool->getBase();
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.writeUnmodified("CREATE TABLE recoveryReplication (id INTEGER PRIMARY KEY);"));
            ASSERT_TRUE(db.prepare());
            ASSERT_EQUAL(db.commit(), SQLITE_OK);
            before = db.getCommitCount();
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            node._changeState(SQLiteNodeState::FOLLOWING);
            SQLitePeer* peer = node.getPeerByName("peer1");
            node._leadPeer = peer;
            peer->loggedIn = true;
            later = transaction(before + 2, "INSERT INTO recoveryReplication VALUES (1);", before);
            node._onMESSAGE(peer, later);
            bool applied;
            {
                unique_lock<mutex> lock(node._replicateMutex);
                applied = node._replicateCV.wait_for(lock, chrono::seconds(5), [&]() {
                    return db.getOutOfOrderCommitHash(before + 2).has_value();
                });
            }
            EXPECT_TRUE(applied);
        }
        // Reopen the same filename with fresh metadata after HC-Tree rolls back the gapped tail.
        dbPool.reset();
        dbPool = make_shared<SQLitePool>(32, filename, 1000000, 5000, 0, 0, true);
        SQLite& db = dbPool->getBase();
        EXPECT_EQUAL(db.getCommitCount(), before);
        EXPECT_FALSE(db.getOutOfOrderCommitHash(before + 2).has_value());
        EXPECT_FALSE(db.getCommit(before + 2));
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
        node._changeState(SQLiteNodeState::FOLLOWING);
        SQLitePeer* peer = node.getPeerByName("peer1");
        node._leadPeer = peer;
        peer->loggedIn = true;
        node._onMESSAGE(peer, transaction(before + 1, "", before));
        node._onMESSAGE(peer, later);
        node._changeState(SQLiteNodeState::SEARCHING);
        EXPECT_EQUAL(db.getCommitCount(), before + 2);
        EXPECT_EQUAL(db.getCommittedHash(), later["NewHash"]);
        sqlite3_int64 snapshot = 0;
        EXPECT_EQUAL(sqlite3_hct_journal_snapshot(db.getDBHandle(), &snapshot), SQLITE_OK);
        EXPECT_EQUAL(static_cast<uint64_t>(snapshot), before + 2);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM recoveryReplication;"), "1");
        // Application reads use a pooled handle, separate from the synchronization connection.
        SQLiteScopedHandle reader(*dbPool, dbPool->getIndex());
        EXPECT_EQUAL(reader.db().read("SELECT COUNT(*) FROM recoveryReplication;"), "1");
    }

    void testReplicationFailure(const string& failure)
    {
        for (const string mode : {"sync", "subscription", "live"}) {
            const bool live = mode == "live";
            const bool subscribing = mode == "subscription";
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, maxOutstandingWALFrames, 1000000000, "1.0");
            SQLite& db = dbPool->getBase();
            SQLite other(db);
            SQLitePeer* peer = node.getPeerByName("peer1");

            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.write("CREATE TABLE IF NOT EXISTS syncTest (id INTEGER PRIMARY KEY);"));
            ASSERT_TRUE(db.write("DELETE FROM syncTest;"));
            ASSERT_TRUE(db.prepare());
            ASSERT_EQUAL(db.commit(), SQLITE_OK);
            const uint64_t commitCount = db.getCommitCount();
            const string committedHash = db.getCommittedHash();

            // Prepare a valid wire payload without advancing the receiver's committed state.
            const string query = "INSERT INTO syncTest VALUES (1);";
            const string guid = "00000000000000000000000000aBcDeF";
            const string expectedHash = guid + ":" + SToHex(SHashSHA1(guid + query));
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.writeUnmodified(query));
            string hash;
            ASSERT_TRUE(db.prepare(nullptr, &hash, chrono::hours(24), nullptr, guid));
            EXPECT_EQUAL(hash, expectedHash);
            SData commit("COMMIT");
            commit["CommitIndex"] = to_string(commitCount + 1);
            commit["Hash"] = hash;
            commit.content = db.getUncommittedQuery();
            db.rollback();
            const SData validCommit = commit;

            SData response(subscribing ? "SUBSCRIPTION_APPROVED" : "SYNCHRONIZE_RESPONSE");
            response["CommitCount"] = commit["CommitIndex"];
            response["Hash"] = hash;
            response["NumCommits"] = "1";
            response.content = commit.serialize();

            const SQLiteNodeState state = live ? SQLiteNodeState::FOLLOWING :
                subscribing ? SQLiteNodeState::SUBSCRIBING : SQLiteNodeState::SYNCHRONIZING;
            node._changeState(state);
            peer->loggedIn = true;
            if (live || subscribing) {
                node._leadPeer = peer;
            } else {
                node._syncPeer = peer;
            }
            if (failure == "commit") {
                db.setCommitEnabled(false);
            } else if (failure == "write") {
                commit.content = query + "INSERT INTO missingSyncTable VALUES (1);";
            } else if (failure == "constraint") {
                commit.content = query + query;
            } else if (failure == "guid-value") {
                commit["Hash"][0] = '1';
            } else if (failure == "guid-digest") {
                commit["Hash"].back() = hash.back() == '0' ? '1' : '0';
            } else if (failure == "guid-query") {
                commit.content = "INSERT INTO syncTest VALUES (2);";
            } else if (failure == "guid-separator") {
                commit["Hash"][31] = ':';
                commit["Hash"][32] = '0';
            } else if (failure == "guid-no-separator") {
                commit["Hash"].erase(32, 1);
            } else if (failure == "legacy") {
                commit["Hash"] = SToHex(SHashSHA1(committedHash + query));
            } else if (failure == "blank-query") {
                commit["Hash"].clear();
            } else if (failure == "guid-short" || failure == "guid-long" || failure == "guid-nonhex") {
                const string invalidGUID = failure == "guid-short" ? guid.substr(1) :
                    failure == "guid-long" ? guid + "0" : guid.substr(0, 31) + "g";
                // The digest is correct for this invalid GUID, so rejection must not rely on a hash mismatch alone.
                commit["Hash"] = invalidGUID + ":" + SToHex(SHashSHA1(invalidGUID + query));
            } else {
                commit["Hash"].back() = hash.back() == '0' ? '1' : '0';
            }
            string replicationError;
            auto receive = [&](const SData& incoming) {
                if (!live) {
                    SData incomingResponse = response;
                    incomingResponse.content = incoming.serialize();
                    node._onMESSAGE(peer, incomingResponse);
                    return;
                }
                SData transaction("TRANSACTION");
                transaction["ID"] = incoming["CommitIndex"];
                transaction["NewCount"] = incoming["CommitIndex"];
                transaction["NewHash"] = incoming["Hash"];
                transaction.content = incoming.content;
                try {
                    node._handleBeginTransaction(db, peer, transaction);
                    if (!node._handlePrepareTransaction(db, peer, transaction, STimeNow())) {
                        STHROW("prepare failed");
                    }
                    if (node._handleCommitTransaction(db, peer, transaction.calcU64("NewCount"), transaction["NewHash"]) != SQLITE_OK) {
                        STHROW("commit failed");
                    }
                } catch (const exception& e) {
                    // Exercise the same cleanup as the replication worker, without starting an asynchronous thread.
                    replicationError = e.what();
                    node._onReplicationError(db, peer, transaction, e.what());
                }
            };
            receive(commit);

            if (live && failure != "blank-query") {
                EXPECT_TRUE(db.getLastTransactionType() == (db.isExperimentalHCTree() ?
                    SQLite::TRANSACTION_TYPE::SHARED : SQLite::TRANSACTION_TYPE::EXCLUSIVE));
            }
            EXPECT_TRUE(node.getState() == (live ? SQLiteNodeState::FOLLOWING : SQLiteNodeState::SEARCHING));
            if (live && (failure == "hash" || failure == "guid-value" || failure == "guid-digest" || failure == "guid-query")) {
                EXPECT_TRUE(SContains(replicationError, "hash mismatch:"));
            }
            if (!live) {
                EXPECT_EQUAL(node._syncPeer, nullptr);
                EXPECT_EQUAL(node._leadPeer.load(), nullptr);
                EXPECT_FALSE(peer->loggedIn);
            }
            EXPECT_FALSE(db.insideTransaction());
            EXPECT_TRUE(sqlite3_get_autocommit(db.getDBHandle()));
            EXPECT_TRUE(db.getUncommittedHash().empty());
            EXPECT_TRUE(db.getUncommittedQuery().empty());
            EXPECT_EQUAL(db.getCommitCount(), commitCount);
            EXPECT_EQUAL(db.getCommittedHash(), committedHash);

            // The commit mutex is recursive, so only another thread can detect a leaked lock.
            bool prepared = false;
            thread waiter([&]() {
                if (other.beginTransaction()) {
                    prepared = other.prepare(nullptr, nullptr, chrono::milliseconds(250));
                }
                other.rollback();
            });
            waiter.join();
            EXPECT_TRUE(prepared);
            ASSERT_FALSE(db.insideTransaction());
            EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM syncTest;"), "0");

            // Retry the valid response on the same handle without any test-side rollback.
            db.setCommitEnabled(true);
            node._changeState(state);
            peer->loggedIn = true;
            if (live || subscribing) {
                node._leadPeer = peer;
            } else {
                node._syncPeer = peer;
            }
            receive(validCommit);
            EXPECT_TRUE(node.getState() == (live || subscribing ? SQLiteNodeState::FOLLOWING : SQLiteNodeState::WAITING));
            EXPECT_EQUAL(db.getCommitCount(), commitCount + 1);
            EXPECT_EQUAL(db.getCommittedHash(), hash);
            EXPECT_FALSE(db.insideTransaction());
            EXPECT_TRUE(db.getUncommittedHash().empty());
            EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM syncTest;"), "1");
            string storedHash;
            ASSERT_TRUE(db.getCommit(commitCount + 1, nullptr, &storedHash));
            EXPECT_EQUAL(storedHash, hash);
        }
    }
} __SQLiteNodeTest;
