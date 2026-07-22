#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

// Verifies that a command which issues an optional HTTPS request from the serialized blockingCommit thread (worker 0)
// is refused rather than allowed to stall the blocking queue on a network round-trip. See
// https://github.com/Expensify/Expensify/issues/661688.
//
// The `httpsblockingcommit` test command only queues an HTTPS request when it runs on the blockingCommit thread; on the
// normal worker threads it is a plain conflicting write. By setting MaxConflictRetries low and firing many concurrent
// copies that collide on the same key, some commands are escalated to the blocking queue and re-run on worker 0, where
// the guard must refuse them with "500 Refused".
struct HTTPSBlockingCommitTest : tpunit::TestFixture
{
    HTTPSBlockingCommitTest()
        : tpunit::TestFixture("HTTPSBlockingCommit",
                              BEFORE_CLASS(HTTPSBlockingCommitTest::setup),
                              TEST(HTTPSBlockingCommitTest::testRefusedViaPreCheck),
                              TEST(HTTPSBlockingCommitTest::testRefusedViaPeekThrow),
                              AFTER_CLASS(HTTPSBlockingCommitTest::teardown))
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester();

        // Escalate to the blocking queue after a single conflict so we reliably reach worker 0 under contention.
        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        tester->getTester(0).executeWaitVerifyContent(setConflict, "200", true);
    }

    void teardown()
    {
        // Restore the default so we don't affect other tests sharing the fixture binary.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        tester->getTester(0).executeWaitVerifyContent(resetConflict, "200", true);

        delete tester;
    }

    // Fires many concurrent, mutually-conflicting `httpsblockingcommit` commands across all nodes and asserts that at
    // least one came back refused because it issued its HTTPS request on the blockingCommit thread. Every request must
    // get a response (a normal 200 or a refusal); nothing should hang.
    void runAndVerifyRefusals(bool waitInPeek)
    {
        const string refusedPrefix = "500 Refused - HTTPS request attempted on blockingCommit thread";
        atomic<int> refused(0);
        atomic<int> total(0);
        list<thread> threads;

        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, waitInPeek, refusedPrefix, &refused, &total]() {
                BedrockTester& node = tester->getTester(i);

                vector<SData> requests;
                for (int j = 0; j < 100; j++) {
                    SData cmd("httpsblockingcommit");
                    cmd["writeConsistency"] = "ASYNC";
                    cmd["value"] = "node" + to_string(i) + "-" + to_string(j);
                    if (waitInPeek) {
                        cmd["waitInPeek"] = "true";
                    }
                    requests.push_back(cmd);
                }

                auto results = node.executeWaitMultipleData(requests);
                for (auto& result : results) {
                    total.fetch_add(1);
                    if (SStartsWith(result.methodLine, refusedPrefix)) {
                        refused.fetch_add(1);
                    }
                }
            });
        }

        for (thread& t : threads) {
            t.join();
        }

        ASSERT_EQUAL(total.load(), 300);
        ASSERT_TRUE(refused.load() > 0);
    }

    // Command returns false from peek without waiting; the worker loop's pre-check refuses it before waiting.
    void testRefusedViaPreCheck()
    {
        runAndVerifyRefusals(false);
    }

    // Command waits from within peek; the throw in _waitForHTTPSRequests is caught by peekCommand and turned into the
    // same refusal response.
    void testRefusedViaPeekThrow()
    {
        runAndVerifyRefusals(true);
    }
} __HTTPSBlockingCommitTest;
