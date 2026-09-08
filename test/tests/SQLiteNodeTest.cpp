/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SQLiteNodeTest.cpp
 * Path:    test/tests/SQLiteNodeTest.cpp
 * Pair:    (declarations in sqlitecluster/SQLiteNode.h)
 *
 * INTENT
 *   Unit tests for SQLiteNode's sync-peer selection logic (the
 *   fastest logged-in peer by latency, tie-broken by commit count) and
 *   for name-based peer lookup.
 *
 * OBJECTS
 *   SQLiteNodeTester - white-box test accessor exposing SQLiteNode's
 *     private _syncPeer and _updateSyncPeer() (granted friend access
 *     in SQLiteNode.h).
 *   TestServer - minimal SQLiteServer stub with no-op overrides, needed
 *     only so a SQLiteNode can be constructed.
 *   SQLiteNodeTest - tpunit fixture (BEFORE_CLASS/AFTER_CLASS-managed
 *     temp DB file and SQLitePool) with 2 tests: sync-peer re-selection
 *     as peer latency and login state change, and getPeerByName()
 *     lookup including on an unsorted peer list.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent; SQLiteNodeTester's name makes clear it's a test-only
 *   accessor rather than production code.
 * ─────────────────────────────────────────────────────────────────────*/

#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteCommand.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLitePeer.h>
#include <sqlitecluster/SQLiteServer.h>
#include <test/lib/BedrockTester.h>

#include <unistd.h>
#include <cstring>

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
                                           TEST(SQLiteNodeTest::testFindSyncPeer),
                                           TEST(SQLiteNodeTest::testGetPeerByName))
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
        dbPool = make_shared<SQLitePool>(10, filename, 1000000, 5000, 0);
    }

    void teardown()
    {
        unlink(filename);
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
} __SQLiteNodeTest;
