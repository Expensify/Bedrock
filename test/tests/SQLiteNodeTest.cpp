#include <libstuff/libstuff.h>
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
                                           TEST(SQLiteNodeTest::testSynchronizeCommitFailure),
                                           TEST(SQLiteNodeTest::testSynchronizeWriteFailure),
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
        testSynchronizeFailure("commit");
    }

    void testSynchronizeWriteFailure()
    {
        testSynchronizeFailure("write");
    }

    void testSynchronizeHashMismatch()
    {
        testSynchronizeFailure("hash");
    }

    void testSynchronizeFailure(const string& failure)
    {
        for (bool subscribing : {false, true}) {
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
            ASSERT_TRUE(db.beginTransaction());
            ASSERT_TRUE(db.writeUnmodified(query));
            string hash;
            ASSERT_TRUE(db.prepare(nullptr, &hash));
            SData commit("COMMIT");
            commit["CommitIndex"] = to_string(commitCount + 1);
            commit["Hash"] = hash;
            commit.content = db.getUncommittedQuery();
            db.rollback();

            SData response(subscribing ? "SUBSCRIPTION_APPROVED" : "SYNCHRONIZE_RESPONSE");
            response["CommitCount"] = commit["CommitIndex"];
            response["Hash"] = hash;
            response["NumCommits"] = "1";
            response.content = commit.serialize();

            const SQLiteNodeState state = subscribing ? SQLiteNodeState::SUBSCRIBING : SQLiteNodeState::SYNCHRONIZING;
            node._changeState(state);
            peer->loggedIn = true;
            if (subscribing) {
                node._leadPeer = peer;
            } else {
                node._syncPeer = peer;
            }
            if (failure == "commit") {
                db.setCommitEnabled(false);
            } else if (failure == "write") {
                commit.content = query + "INSERT INTO missingSyncTable VALUES (1);";
            } else {
                commit["Hash"] = "incorrect hash";
            }
            SData failedResponse = response;
            failedResponse.content = commit.serialize();
            node._onMESSAGE(peer, failedResponse);

            EXPECT_TRUE(node.getState() == SQLiteNodeState::SEARCHING);
            EXPECT_EQUAL(node._syncPeer, nullptr);
            EXPECT_EQUAL(node._leadPeer.load(), nullptr);
            EXPECT_FALSE(peer->loggedIn);
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
            node._changeState(state);
            peer->loggedIn = true;
            if (subscribing) {
                node._leadPeer = peer;
            } else {
                node._syncPeer = peer;
            }
            node._onMESSAGE(peer, response);
            EXPECT_TRUE(node.getState() == (subscribing ? SQLiteNodeState::FOLLOWING : SQLiteNodeState::WAITING));
            EXPECT_EQUAL(db.getCommitCount(), commitCount + 1);
            EXPECT_EQUAL(db.getCommittedHash(), hash);
            EXPECT_FALSE(db.insideTransaction());
            EXPECT_TRUE(db.getUncommittedHash().empty());
            EXPECT_EQUAL(db.read("SELECT COUNT(*) FROM syncTest;"), "1");
        }
    }
} __SQLiteNodeTest;
