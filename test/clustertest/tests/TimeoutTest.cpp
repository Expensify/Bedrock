/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    TimeoutTest.cpp
 * Path:    test/clustertest/tests/TimeoutTest.cpp
 *
 * INTENT
 *   Cluster-level tpunit test fixture covering command timeouts across
 *   every stage (peek, process, postProcess, total, HTTPS request,
 *   future-commit wait) and the behavior of client disconnects: an
 *   abandoned command must roll back and free its slot, while a
 *   fire-and-forget command must keep running after its socket closes.
 *
 * OBJECTS
 *   sendRequestAndDisconnect (file-local static free function) - opens a
 *       raw socket to a tester's command port, writes one request, waits
 *       `holdForUS`, then closes the socket without reading a response,
 *       simulating a client that disconnects mid-command.
 *   TimeoutTest (struct, extends tpunit::TestFixture) - registers each
 *       test method below against a single default BedrockClusterTester.
 *   TimeoutTest::setup/teardown - allocate/free the cluster tester.
 *   TimeoutTest::test/longerThanDefaultProcess - assert `slowquery`
 *       times out during peek, including past the default process
 *       timeout when only the peek timeout is set.
 *   TimeoutTest::testprocess/testPostProcess - assert `slowprocessquery`/
 *       `testPostProcessTimeout` time out at their respective stages.
 *   TimeoutTest::totalTimeout/httpsRequestTimeout/futureCommitTimeout -
 *       assert timeout on an unanswered HTTPS request, on a request that
 *       is never sent, and on waiting for a commit count that never
 *       arrives.
 *   TimeoutTest::abortDuringProcessRollsBack - disconnects mid-write via
 *       sendRequestAndDisconnect and asserts the command is abandoned
 *       (commandCount falls back to 1) and its write is rolled back.
 *   TimeoutTest::abortDuringEscalationRollsBack - same, but sent to a
 *       follower so the abort must propagate through escalation to the
 *       leader, where the row-count check is actually performed.
 *   TimeoutTest::forgetCommandNotAborted - asserts a `Connection: forget`
 *       command keeps running and eventually commits after its socket
 *       closes, rather than being aborted like a real disconnect.
 *
 * OUT OF PLACE
 *   Nothing - every method targets some facet of timeout/abort behavior.
 *
 * NAME/LOCATION FIT
 *   Fits; a cluster-level timeout/abort test under test/clustertest/tests.
 *
 * NAMING QUALITY
 *   Method names are specific and describe the scenario under test;
 *   consistent with sibling files in this directory.
 * ─────────────────────────────────────────────────────────────────────*/
#include "test/lib/tpunit++.hpp"
#include <BedrockCommand.h>
#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <libstuff/SFastBuffer.h>
#include <libstuff/SQResult.h>
#include <test/clustertest/BedrockClusterTester.h>

// Opens a connection to a tester's command port, sends a single request, waits `holdForUS` for the server to begin
// handling it, then closes the socket without reading a response -- simulating a client that disconnects while its
// command is still in flight.
static void sendRequestAndDisconnect(BedrockTester& tester, const SData& request, uint64_t holdForUS = 250'000)
{
    int socket = S_socket(tester.getArg("-serverHost"), true, false, true);
    if (socket == -1) {
        STHROW("sendRequestAndDisconnect: failed to open socket.");
    }

    SFastBuffer sendBuffer(request.serialize());
    while (sendBuffer.size()) {
        if (!S_sendconsume(socket, sendBuffer)) {
            S_close(&socket);
            STHROW("sendRequestAndDisconnect: failed to send request.");
        }
    }

    // Give the server time to read the request and start handling the command, then disconnect.
    usleep(holdForUS);
    S_close(&socket);
}

struct TimeoutTest : tpunit::TestFixture
{
    TimeoutTest()
        : tpunit::TestFixture("Timeout",
                              BEFORE_CLASS(TimeoutTest::setup),
                              AFTER_CLASS(TimeoutTest::teardown),
                              TEST(TimeoutTest::test),
                              TEST(TimeoutTest::longerThanDefaultProcess),
                              TEST(TimeoutTest::testprocess),
                              TEST(TimeoutTest::testPostProcess),
                              TEST(TimeoutTest::totalTimeout),
                              TEST(TimeoutTest::httpsRequestTimeout),
                              TEST(TimeoutTest::futureCommitTimeout),
                              TEST(TimeoutTest::abortDuringProcessRollsBack),
                              TEST(TimeoutTest::abortDuringEscalationRollsBack),
                              TEST(TimeoutTest::forgetCommandNotAborted))
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    void test()
    {
        BedrockTester& brtester = tester->getTester(0);

        // Run one long query.
        SData slow("slowquery");
        slow["timeout"] = "1000"; // 1s
        brtester.executeWaitVerifyContent(slow, "555 Timeout peeking command");

        // And a bunch of faster ones.
        slow["size"] = "10000";
        slow["count"] = "10000";
        brtester.executeWaitVerifyContent(slow, "555 Timeout peeking command");
    }

    void longerThanDefaultProcess()
    {
        BedrockTester& brtester = tester->getTester(0);

        // Run a (read-only) query that takes longer than the default process timeout, without changing the process
        // timeout.
        SData slow("slowquery");
        slow["size"] = "1000000000";
        slow["timeout"] = to_string(BedrockCommand::DEFAULT_PROCESS_TIMEOUT + 5'000);
        auto start = STimeNow();
        brtester.executeWaitVerifyContent(slow, "555 Timeout peeking command");
        auto end = STimeNow();
        ASSERT_GREATER_THAN_EQUAL((end - start) / 1000, BedrockCommand::DEFAULT_PROCESS_TIMEOUT + 5'000);
    }

    void httpsRequestTimeout()
    {
        BedrockTester& brtester = tester->getTester(0);
        SData request("httpstimeout");
        request["timeout"] = "100"; // 100ms.
        brtester.executeWaitVerifyContent(request, "555 Timeout");
    }

    void testprocess()
    {
        // Test write commands.
        BedrockTester& brtester = tester->getTester(0);

        // Run one long query.
        SData slow("slowprocessquery");
        slow["processTimeout"] = "200"; // 0.2s
        slow["size"] = "1000000";
        slow["count"] = "1";
        brtester.executeWaitVerifyContent(slow, "555 Timeout processing command");

        // And a bunch of faster ones.
        slow["size"] = "100";
        slow["count"] = "10000";
        brtester.executeWaitVerifyContent(slow, "555 Timeout processing command");
    }

    void testPostProcess()
    {
        BedrockTester& brtester = tester->getTester(0);
        SData slow("testPostProcessTimeout");
        slow["timeout"] = "500"; // 0.5s
        brtester.executeWaitVerifyContent(slow, "555 Timeout postProcessing command");
    }

    void totalTimeout()
    {
        // Test total timeout, not process timeout.
        BedrockTester& brtester = tester->getTester(0);

        SData https("httpstimeout");
        https["timeout"] = "5000"; // 5s.
        https["neversend"] = "1";
        brtester.executeWaitVerifyContent(https, "555 Timeout");
    }

    void futureCommitTimeout()
    {
        // Test total timeout, not process timeout.
        BedrockTester& brtester = tester->getTester(0);

        SData https("Query");
        https["timeout"] = "5000"; // 5s.
        https["commitCount"] = "10000000000";
        brtester.executeWaitVerifyContent(https, "555 Timeout");
    }

    void abortDuringProcessRollsBack()
    {
        // When the client that sent a command disconnects while the command is still being processed, the command
        // should be aborted and its (uncommitted) write rolled back rather than committed.
        BedrockTester& brtester = tester->getTester(0);

        SQResult before;
        brtester.readDB("SELECT COUNT(*) FROM test;", before);
        ASSERT_TRUE(before.size());
        int64_t rowsBefore = SToInt64(before[0][0]);

        // A slow write, sized so it's still running a couple of seconds after we disconnect. The timeouts are set
        // high (well past how long it takes to finish) on purpose: if abort-on-disconnect were broken, the command
        // would run to completion and *write its rows* -- caught by the row-count assertion below -- rather than
        // quietly rolling back via a 555 timeout, which would let a regression pass unnoticed.
        SData slow("slowprocessquery");
        slow["size"] = "10000";
        slow["count"] = "2000";
        slow["timeout"] = "120000";        // 120s
        slow["processTimeout"] = "120000"; // 120s
        sendRequestAndDisconnect(brtester, slow);

        // Once the disconnect is noticed, the command is abandoned and destroyed, leaving only the Status command we
        // poll with alive. A `commandCount` of 1 means nothing else is running. We bound the wait well under the
        // command's timeout: if the abort didn't happen, the command would still be running when this gives up.
        ASSERT_TRUE(brtester.waitForStatusTerm("commandCount", "1", 30'000'000));

        // The aborted write must have been rolled back: the row count is unchanged.
        SQResult afterAbort;
        brtester.readDB("SELECT COUNT(*) FROM test;", afterAbort);
        ASSERT_EQUAL(SToInt64(afterAbort[0][0]), rowsBefore);
    }

    void forgetCommandNotAborted()
    {
        // A fire-and-forget command (`Connection: forget`) gets a 202 and the server closes its socket immediately.
        // That close must NOT abort the command -- with no client awaiting a reply, it should still run to completion
        // in the background.
        BedrockTester& brtester = tester->getTester(0);

        SQResult before;
        brtester.readDB("SELECT COUNT(*) FROM test;", before);
        ASSERT_TRUE(before.size());
        int64_t rowsBefore = SToInt64(before[0][0]);

        SData forget("slowprocessquery");
        forget["size"] = "100";
        forget["count"] = "1";
        forget["Connection"] = "forget";
        brtester.executeWaitVerifyContent(forget, "202");

        // The command runs after its socket closed, so poll until its rows appear.
        int64_t rowsAfter = rowsBefore;
        uint64_t start = STimeNow();
        while (STimeNow() < start + 10'000'000) {
            SQResult after;
            brtester.readDB("SELECT COUNT(*) FROM test;", after);
            rowsAfter = SToInt64(after[0][0]);
            if (rowsAfter > rowsBefore) {
                break;
            }
            usleep(100'000);
        }
        ASSERT_GREATER_THAN(rowsAfter, rowsBefore);
    }

    void abortDuringEscalationRollsBack()
    {
        // Like abortDuringProcessRollsBack, but the command is sent to a *follower*, which escalates it to the leader
        // (via SQLiteClusterMessenger::runOnPeer). Disconnecting the client should cancel the command all the way
        // through the escalation chain: the follower drops its connection to the leader, and the leader then aborts
        // the escalated command and rolls back its write.
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);
        ASSERT_TRUE(leader.waitForState("LEADING"));
        ASSERT_TRUE(follower.waitForState("FOLLOWING"));

        // Row counts are observed on the leader, where the escalated write actually executes.
        SQResult before;
        leader.readDB("SELECT COUNT(*) FROM test;", before);
        ASSERT_TRUE(before.size());
        int64_t rowsBefore = SToInt64(before[0][0]);

        SData slow("slowprocessquery");
        slow["size"] = "10000";
        slow["count"] = "2000";
        slow["timeout"] = "120000";        // 120s
        slow["processTimeout"] = "120000"; // 120s
        sendRequestAndDisconnect(follower, slow);

        // The escalated command should be abandoned on the *leader*, not just the follower. Wait for the leader's
        // commandCount to fall back to 1 (only the Status command we poll with is left), bounded well under the
        // command's timeout so a failure to propagate the abort to the leader is caught here.
        ASSERT_TRUE(leader.waitForStatusTerm("commandCount", "1", 30'000'000));

        // The aborted write must have been rolled back on the leader.
        SQResult afterAbort;
        leader.readDB("SELECT COUNT(*) FROM test;", afterAbort);
        ASSERT_EQUAL(SToInt64(afterAbort[0][0]), rowsBefore);
    }
} __TimeoutTest;
