#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct TimeoutTest : tpunit::TestFixture {
    TimeoutTest()
        : tpunit::TestFixture("TimeoutTest",
                              BEFORE_CLASS(TimeoutTest::setup),
                              AFTER_CLASS(TimeoutTest::teardown),
                              TEST(TimeoutTest::test),
                              TEST(TimeoutTest::testprocess),
                              TEST(TimeoutTest::totalTimeout),
                              TEST(TimeoutTest::quorumHTTPS),
                              TEST(TimeoutTest::futureCommitTimeout)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester();
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        // Test write commands.
        BedrockTester& brtester = tester->getTester(0);

        // Run one long query.
        SData slow("slowquery");
        slow["processTimeout"] = "1000"; // 1s
        brtester.executeWaitVerifyContent(slow, "555 Timeout peeking command");

        // And a bunch of faster ones.
        slow["size"] = "10000";
        slow["count"] = "10000";
        brtester.executeWaitVerifyContent(slow, "555 Timeout peeking command");
    }

    void quorumHTTPS () {
        BedrockTester& brtester = tester->getTester(0);
        SData request("httpstimeout");
        request["writeConsistency"] = "2"; // QUORUM.
        request["timeout"] = "100"; // 100ms.
        brtester.executeWaitVerifyContent(request, "555 Timeout");
    }

    void testprocess()
    {
        // Test write commands.
        BedrockTester& brtester = tester->getTester(0);

        // Run one long query.
        SData slow("slowprocessquery");
        slow["processTimeout"] = "500"; // 0.5s
        slow["size"] = "1000000";
        slow["count"] = "1";
        brtester.executeWaitVerifyContent(slow, "555 Timeout processing command");

        // And a bunch of faster ones.
        slow["size"] = "100";
        slow["count"] = "10000";
        brtester.executeWaitVerifyContent(slow, "555 Timeout processing command");
    }

    void totalTimeout() {
        // Test total timeout, not process timeout.
        BedrockTester& brtester = tester->getTester(0);

        SData https("httpstimeout");
        https["timeout"] = "5000"; // 5s.
        https["neversend"] = "1";
        brtester.executeWaitVerifyContent(https, "555 Timeout");
    }

    void futureCommitTimeout() {
        // Test total timeout, not process timeout.
        BedrockTester& brtester = tester->getTester(0);

        SData https("Query");
        https["timeout"] = "5000"; // 5s.
        https["commitCount"] = "10000000000";
        brtester.executeWaitVerifyContent(https, "555 Timeout");
    }

} __TimeoutTest;

