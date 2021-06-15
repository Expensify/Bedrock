#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteCommand.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLiteServer.h>
#include <test/lib/BedrockTester.h>

#include <unistd.h>
#include <cstring>

class SQLiteNodeTester {
  public:
    static SQLiteNode::Peer* getSyncPeer(SQLiteNode& node) {
        return node._syncPeer;
    }

    static void updateSyncPeer(SQLiteNode& node) {
        node._updateSyncPeer();
    }
};

class TestServer : public SQLiteServer {
  public:
    TestServer(const string& host) : SQLiteServer(host) { }

    virtual void acceptCommand(unique_ptr<SQLiteCommand>&& command, bool isNew) { }
    virtual void cancelCommand(const string& commandID) { }
    virtual bool canStandDown() { return true; }
    virtual void onNodeLogin(SQLiteNode::Peer* peer) { }
};

struct SQLiteNodeTest : tpunit::TestFixture {
    SQLiteNodeTest() : tpunit::TestFixture("SQLiteNode",
                                           AFTER_CLASS(SQLiteNodeTest::teardown),
                                           TEST(SQLiteNodeTest::testFindSyncPeer)) { }

    // Filename for temp DB.
    char filenameTemplate[17] = "br_sync_dbXXXXXX";
    char filename[17];

    void teardown() {
        unlink(filename);
    }

    void testFindSyncPeer() {

        // This exposes just enough to test the peer selection logic.
        strcpy(filename, filenameTemplate);
        int fd = mkstemp(filename);
        close(fd);
        SQLitePool dbPool(10, filename, 1000000, 5000, 0);
        TestServer server("");
        string peerList = "host1.fake:15555?nodeName=peer1,host2.fake:16666?nodeName=peer2,host3.fake:17777?nodeName=peer3,host4.fake:18888?nodeName=peer4";
        SQLiteNode testNode(server, dbPool, "test", "localhost:19998", peerList, 1, 1000000000, "1.0");

        // Do a base test, with one peer with no latency.
        SQLiteNode::Peer* fastest = nullptr;
        for (auto peer : testNode.peerList) {
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
        for (auto peer : testNode.peerList) {
            // New fastest is peer 3.
            if (peer->name == "peer3") {
                peer->latency = 50;
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);

        // And see what happens if our fastest peer logs out.
        for (auto peer : testNode.peerList) {
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
        for (auto peer : testNode.peerList) {
            // New fastest is peer 3.
            if (peer->name == "peer1") {
                peer->latency = 75;
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);

        // Now none of our peers have latency data, but one has more commits.
        for (auto peer : testNode.peerList) {
            peer->latency = 0;

            // 4 had highest commit count.
            if (peer->name == "peer4") {
                fastest = peer;
            }
        }
        SQLiteNodeTester::updateSyncPeer(testNode);
        ASSERT_EQUAL(SQLiteNodeTester::getSyncPeer(testNode), fastest);
    }

} __SQLiteNodeTest;
