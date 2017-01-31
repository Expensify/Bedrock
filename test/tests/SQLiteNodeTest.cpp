#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>

struct TestSQLiteNode : public SQLiteNode {
    TestSQLiteNode() : SQLiteNode(":memory:", "test", "localhost:9999", 1, 1, 1, 1000000000, "1.0", -1, 0) { }

    // Useless implementations that at least define the methods.
    bool _peekCommand(SQLite&, SQLiteNode::Command*) {return true;}
    void _processCommand(SQLite&, SQLiteNode::Command*) {}
    void _abortCommand(SQLite&, SQLiteNode::Command*) {}
    void _cleanCommand(SQLiteNode::Command*) {}

    void updateSyncPeer() {_updateSyncPeer();}
    Peer* getSyncPeer() {return _syncPeer;}
};

struct SQLiteNodeTest : tpunit::TestFixture {
    SQLiteNodeTest() : tpunit::TestFixture("SQLiteNode",
                                           TEST(SQLiteNodeTest::testFindSyncPeer)) { }

    void testFindSyncPeer() {

        // This exposes just enough to test the peer selection logic.
        TestSQLiteNode testNode;

        STable dummyParams;
        testNode.addPeer("peer1", "host1.fake:5555", dummyParams);
        testNode.addPeer("peer2", "host2.fake:6666", dummyParams);
        testNode.addPeer("peer3", "host3.fake:7777", dummyParams);
        testNode.addPeer("peer4", "host4.fake:8888", dummyParams);

        // Do a base test, with one peer with no latency.
        SQLiteNode::Peer* fastest = nullptr;
        for (auto peer : testNode.peerList) {
            int peerNum = peer->name[4] - 48;
            (*peer)["LoggedIn"] = "true";
            (*peer)["CommitCount"] = to_string(10000000 + peerNum);

            // 0, 100, 200, 300.
            peer->latency = (peerNum - 1) * 100;

            // Our fastest should be `peer2`, it has lowest non-zero latency.
            if (peer->name == "peer2") {
                fastest = peer;
            }
        }
        testNode.updateSyncPeer();
        ASSERT_EQUAL(testNode.getSyncPeer(), fastest);

        // See what happens when another peer becomes faster.
        for (auto peer : testNode.peerList) {
            // New fastest is peer 3.
            if (peer->name == "peer3") {
                peer->latency = 50;
                fastest = peer;
            }
        }
        testNode.updateSyncPeer();
        ASSERT_EQUAL(testNode.getSyncPeer(), fastest);

        // And see what happens if our fastest peer logs out.
        for (auto peer : testNode.peerList) {
            if (peer->name == "peer3") {
                (*peer)["LoggedIn"] = "false";
                peer->latency = 50;
            }

            // 2 is fastest again.
            if (peer->name == "peer2") {
                fastest = peer;
            }
        }
        testNode.updateSyncPeer();
        ASSERT_EQUAL(testNode.getSyncPeer(), fastest);

        // And then if our previously 0 latency peer gets (fast) latency data.
        for (auto peer : testNode.peerList) {
            // New fastest is peer 3.
            if (peer->name == "peer1") {
                peer->latency = 75;
                fastest = peer;
            }
        }
        testNode.updateSyncPeer();
        ASSERT_EQUAL(testNode.getSyncPeer(), fastest);

        // Now none of our peers have latency data, but one has more commits.
        for (auto peer : testNode.peerList) {
            peer->latency = 0;

            // 4 had highest commit count.
            if (peer->name == "peer4") {
                fastest = peer;
            }
        }
        testNode.updateSyncPeer();
        ASSERT_EQUAL(testNode.getSyncPeer(), fastest);
    }

} __SQLiteNodeTest;
