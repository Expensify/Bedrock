#include <libstuff/libstuff.h>
#include <plugins/Compression.h>
#include <sqlitecluster/SQLiteCommand.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLitePeer.h>
#include <sqlitecluster/SQLiteServer.h>
#include <test/lib/BedrockTester.h>

#include <unistd.h>
#include <cstring>
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
};

class TestServer : public SQLiteServer {
public:
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
    };
    virtual void unblockCommandPort(const string& reason)
    {
    };
};

struct SQLiteNodeTest : tpunit::TestFixture
{
    SQLiteNodeTest() : tpunit::TestFixture("SQLiteNode",
                                           BEFORE_CLASS(SQLiteNodeTest::setup),
                                           AFTER_CLASS(SQLiteNodeTest::teardown),
                                           AFTER(SQLiteNodeTest::rollback),
                                           TEST(SQLiteNodeTest::testFindSyncPeer),
                                           TEST(SQLiteNodeTest::testGetPeerByName),
                                           TEST(SQLiteNodeTest::testPrepareGUID),
                                           TEST(SQLiteNodeTest::testMixedHashHistory),
                                           TEST(SQLiteNodeTest::testGUIDHashFailures),
                                           TEST(SQLiteNodeTest::testReplicationRequiresLeader),
                                           TEST(SQLiteNodeTest::testSynchronizeCommitFailure),
                                           TEST(SQLiteNodeTest::testSynchronizeWriteFailure),
                                           TEST(SQLiteNodeTest::testSynchronizeConstraintFailure),
                                           TEST(SQLiteNodeTest::testSynchronizeHashMismatch))
    {
    }

    // Filename for temp DB.
    char filename[17] = "br_sync_dbXXXXXX";

    TestServer server;
    atomic<int> configuredPriority{1};
    string peerList = "host1.fake:15555?nodeName=peer1,host2.fake:16666?nodeName=peer2,host3.fake:17777?nodeName=peer3,host4.fake:18888?nodeName=peer4";
    shared_ptr<SQLitePool> dbPool;

    void setup()
    {
        // This exposes just enough to test the peer selection logic.
        int fd = mkstemp(filename);
        close(fd);
        dbPool = make_shared<SQLitePool>(10, filename, 1000000, 5000, 0, 0, BedrockTester::ENABLE_HCTREE);
    }

    void teardown()
    {
        dbPool.reset();
        unlink(filename);
        unlink((string(filename) + "-pagemap").c_str());
        unlink((string(filename) + "-log-0").c_str());
    }

    void rollback()
    {
        // Keep a failed assertion from leaving the fixture's shared handle locked for the next test.
        dbPool->getBase().rollback();
        dbPool->getBase().setCommitEnabled(true);
    }

    void testFindSyncPeer()
    {
        SQLiteNode testNode(server, dbPool, "test", "localhost:19998", peerList, configuredPriority, 1000000000, "1.0");

        // Do a base test, with one peer with no latency.
        SQLitePeer* fastest = nullptr;
        for (auto peer : testNode._peerList) {
            int peerNum = peer->name[4] - 48;
            peer->loggedIn = true;
            peer->setCommit(10000000 + peerNum, "");

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

    void testGetPeerByName()
    {
        {
            SQLiteNode testNode(server, dbPool, "test", "localhost:19998", peerList, configuredPriority, 1000000000, "1.0");
            ASSERT_EQUAL(testNode.getPeerByName("peer3")->name, "peer3");
            ASSERT_EQUAL(testNode.getPeerByName("peer9"), nullptr);
        }
        {
            // It also works when the peer list isn't pre-sorted
            string unsortedPeerList = "host1.fake:15555?nodeName=peerZ,host2.fake:16666?nodeName=peer1,host3.fake:17777?nodeName=peer0,host4.fake:18888?nodeName=peerBanana";
            SQLiteNode testNode(server, dbPool, "test", "localhost:19998", unsortedPeerList, configuredPriority, 1000000000, "1.0");
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

    void testPrepareGUID()
    {
        SQLite& db = dbPool->getBase();
        const uint64_t commitCount = db.getCommitCount();
        const string committedHash = db.getCommittedHash();
        const string query = "CREATE TABLE prepareGUIDTest (id INTEGER);";
        for (const string guid : {"00000000000000000000000000aBcDeF", "", "00000000000000000000000000abcde0"}) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.writeUnmodified(query));
            string hash;
            ASSERT_TRUE(db.prepare(nullptr, &hash, chrono::hours(24), nullptr, guid));
            const string expectedHash = guid.empty() ? SToHex(SHashSHA1(committedHash + query)) :
                guid + ":" + SToHex(SHashSHA1(guid + query));
            EXPECT_EQUAL(hash, expectedHash);
            EXPECT_EQUAL(db.getUncommittedHash(), expectedHash);
            EXPECT_EQUAL(db.getCommittedHash(), committedHash);
            EXPECT_EQUAL(db.getCommitCount(), commitCount);
            db.rollback();
            EXPECT_TRUE(db.getUncommittedHash().empty());
        }
        for (const string& guid : {string(31, '0'), string(33, '0'), string(31, '0') + "g", string(32, '0') + ":" + string(40, '0')}) {
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.writeUnmodified(query));
            EXPECT_FALSE(db.prepare(nullptr, nullptr, chrono::hours(24), nullptr, guid));
            EXPECT_TRUE(db.getUncommittedHash().empty());
            db.rollback();
        }
    }

    void testMixedHashHistory()
    {
        const vector<string> queries = {
            "CREATE TABLE IF NOT EXISTS hashTest (value INTEGER);DELETE FROM hashTest;",
            "INSERT INTO hashTest VALUES (1);",
            "INSERT INTO hashTest VALUES (1);",
            "INSERT INTO hashTest VALUES (2);",
            "",
        };
        const vector<string> guids = {
            "", "00000000000000000000000000aBcDeF", "00000000000000000000000000abcde0", "",
            "00000000000000000000000000000001",
        };
        for (const string mode : {"live", "sync", "subscription"}) {
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, 1000000000, "1.0");
            SQLite& db = dbPool->getBase();
            SQLitePeer* peer = node.getPeerByName("peer1");
            const uint64_t commitCount = db.getCommitCount();
            string previousHash = db.getCommittedHash();
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
                const string hash = guids[i].empty() ? SToHex(SHashSHA1(previousHash + queries[i])) :
                    guids[i] + ":" + SToHex(SHashSHA1(guids[i] + queries[i]));
                hashes.push_back(hash);
                previousHash = hash;
                string content = queries[i];
                // Mix raw and compressed wire SQL without changing the global journal compression setting.
                if (i == 2 || i == 3) {
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
                    EXPECT_EQUAL(db.getUncommittedHash(), hash);
                    ASSERT_EQUAL(node._handleCommitTransaction(db, peer, commitCount + i + 1, hash), SQLITE_OK);
                    EXPECT_EQUAL(db.getCommittedHash(), hash);
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
                response["Hash"] = hashes.back();
                response["NumCommits"] = to_string(queries.size());
                node._onMESSAGE(peer, response);
            }
            EXPECT_TRUE(node.getState() == (live || subscribing ? SQLiteNodeState::FOLLOWING : SQLiteNodeState::WAITING));
            EXPECT_EQUAL(db.getCommitCount(), commitCount + queries.size());
            EXPECT_EQUAL(db.getCommittedHash(), hashes.back());
            EXPECT_NOT_EQUAL(hashes[1], hashes[2]);
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
        ASSERT_EQUAL(committedHash, guids.back() + ":" + SToHex(SHashSHA1(guids.back())));
        // SharedData is cached by filename forever. A fresh path forces the tail to be loaded from the journal.
        char restartedFilename[sizeof(filename)] = "br_sync_dbXXXXXX";
        const int fd = mkstemp(restartedFilename);
        ASSERT_TRUE(fd >= 0);
        close(fd);
        dbPool.reset();
        for (const string suffix : {"", "-pagemap", "-log-0"}) {
            if (suffix.empty() || access((string(filename) + suffix).c_str(), F_OK) == 0) {
                const int result = rename((string(filename) + suffix).c_str(), (string(restartedFilename) + suffix).c_str());
                EXPECT_EQUAL(result, 0);
            }
        }
        memcpy(filename, restartedFilename, sizeof(filename));
        dbPool = make_shared<SQLitePool>(10, filename, 1000000, 5000, 0, 0, BedrockTester::ENABLE_HCTREE);
        SQLite& db = dbPool->getBase();
        EXPECT_EQUAL(db.getCommitCount(), commitCount);
        EXPECT_EQUAL(db.getCommittedHash(), committedHash);
        const string query = "INSERT INTO hashTest VALUES (3);";
        ASSERT_TRUE(db.beginTransaction());
        ASSERT_TRUE(db.writeUnmodified(query));
        ASSERT_TRUE(db.prepare());
        const string legacyHash = SToHex(SHashSHA1(committedHash + query));
        EXPECT_EQUAL(db.getUncommittedHash().size(), (size_t) 40);
        EXPECT_EQUAL(db.getUncommittedHash(), legacyHash);
        ASSERT_EQUAL(db.commit(), SQLITE_OK);
        string storedHash;
        ASSERT_TRUE(db.getCommit(commitCount + 1, nullptr, &storedHash));
        EXPECT_EQUAL(storedHash, legacyHash);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM hashTest;"), "4");
    }

    void testGUIDHashFailures()
    {
        for (const string failure : {"guid-value", "guid-digest", "guid-query", "guid-short", "guid-long", "guid-nonhex", "guid-separator", "guid-no-separator"}) {
            testReplicationFailure(failure);
        }
    }

    void testReplicationRequiresLeader()
    {
        SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, 1000000000, "1.0");
        SQLite& db = dbPool->getBase();
        const uint64_t commitCount = db.getCommitCount();
        const string committedHash = db.getCommittedHash();
        node._changeState(SQLiteNodeState::FOLLOWING);
        node._leadPeer = node.getPeerByName("peer1");
        SQLitePeer* otherPeer = node.getPeerByName("peer2");
        otherPeer->loggedIn = true;

        SData transaction("TRANSACTION");
        transaction["CommitCount"] = to_string(commitCount + 1);
        transaction["NewCount"] = transaction["CommitCount"];
        transaction["ID"] = transaction["CommitCount"];
        transaction.content = "CREATE TABLE nonLeaderTransaction (id INTEGER);";
        const string guid = "00000000000000000000000000000001";
        transaction["NewHash"] = guid + ":" + SToHex(SHashSHA1(guid + transaction.content));
        transaction["Hash"] = transaction["NewHash"];
        node._onMESSAGE(otherPeer, transaction);

        EXPECT_EQUAL(node._replicateThread, nullptr);
        EXPECT_TRUE(node._replicateQueue.empty());
        EXPECT_FALSE(db.insideTransaction());
        EXPECT_EQUAL(db.getCommitCount(), commitCount);
        EXPECT_EQUAL(db.getCommittedHash(), committedHash);
        EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM sqlite_master WHERE name = 'nonLeaderTransaction';"), "0");
    }

    void testReplicationFailure(const string& failure)
    {
        for (const string mode : {"sync", "subscription", "live"}) {
            const bool live = mode == "live";
            const bool subscribing = mode == "subscription";
            SQLiteNode node(server, dbPool, "test", "", peerList, configuredPriority, 1000000000, "1.0");
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
            const string guid = failure.find("guid-") == 0 ? "00000000000000000000000000aBcDeF" : "";
            const string expectedHash = guid.empty() ? SToHex(SHashSHA1(committedHash + query)) :
                guid + ":" + SToHex(SHashSHA1(guid + query));
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
            } else if (failure == "guid-short" || failure == "guid-long" || failure == "guid-nonhex") {
                const string invalidGUID = failure == "guid-short" ? guid.substr(1) :
                    failure == "guid-long" ? guid + "0" : guid.substr(0, 31) + "g";
                // The digest is correct for this invalid GUID, so rejection must not rely on a hash mismatch alone.
                commit["Hash"] = invalidGUID + ":" + SToHex(SHashSHA1(invalidGUID + query));
            } else {
                commit["Hash"] = "incorrect hash";
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

            EXPECT_TRUE(db.getLastTransactionType() == SQLite::TRANSACTION_TYPE::EXCLUSIVE);
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
