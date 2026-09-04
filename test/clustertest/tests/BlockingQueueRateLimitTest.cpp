#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct BlockingQueueRateLimitTest : tpunit::TestFixture
{
    BlockingQueueRateLimitTest()
        : tpunit::TestFixture("BlockingQueueRateLimit",
                              BEFORE_CLASS(BlockingQueueRateLimitTest::setup),
                              BEFORE(BlockingQueueRateLimitTest::before),
                              TEST(BlockingQueueRateLimitTest::testControlCommands),
                              TEST(BlockingQueueRateLimitTest::testTimeRateLimiting),
                              TEST(BlockingQueueRateLimitTest::testGlobalRateLimiting),
                              AFTER_CLASS(BlockingQueueRateLimitTest::teardown))
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

    void before()
    {
        BedrockTester& leader = tester->getTester(0);

        // ClearBlocks should reset all rate limit state.
        SData resetBlockingQueue("SetBlockingQueueTimeRateLimit");
        resetBlockingQueue["ClearBlocks"] = "true";
        leader.executeWaitVerifyContent(resetBlockingQueue, "200", true);

        // Reset state on leader.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        // The global dimension is on by default. Disable it so it can't reject anything in the tests that
        // aren't about it; testGlobalRateLimiting turns it back on.
        SData disableGlobal("SetBlockingQueueTimeRateLimit");
        disableGlobal["globalThresholdMS"] = "0";
        leader.executeWaitVerifyContent(disableGlobal, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingBlockedIdentifiers"], "");
        ASSERT_EQUAL(json["blockingGlobalBlocked"], "false");
    }

    void testControlCommands()
    {
        BedrockTester& leader = tester->getTester(0);

        // Set the window and thresholds and verify they show up in Status.
        SData setLimits("SetBlockingQueueTimeRateLimit");
        setLimits["windowMS"] = "180000";
        setLimits["identifierThresholdMS"] = "20000";
        setLimits["commandThresholdMS"] = "40000";
        setLimits["blockDurationMS"] = "60000";
        setLimits["globalWindowMS"] = "60000";
        setLimits["globalThresholdMS"] = "55000";
        setLimits["globalBlockDurationMS"] = "60000";
        leader.executeWaitVerifyContent(setLimits, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingTimeWindowMS"], "180000");
        ASSERT_EQUAL(json["blockingIdentifierThresholdMS"], "20000");
        ASSERT_EQUAL(json["blockingCommandThresholdMS"], "40000");
        ASSERT_EQUAL(json["blockingBlockDurationMS"], "60000");
        ASSERT_EQUAL(json["blockingGlobalWindowMS"], "60000");
        ASSERT_EQUAL(json["blockingGlobalThresholdMS"], "55000");
        ASSERT_EQUAL(json["blockingGlobalBlockDurationMS"], "60000");
    }

    void testTimeRateLimiting()
    {
        BedrockTester& leader = tester->getTester(0);

        // Small identifier threshold so a burst of conflicting commands trips it. The command dimension is
        // disabled so this isolates the identifier dimension.
        SData setLimits("SetBlockingQueueTimeRateLimit");
        setLimits["windowMS"] = "180000";
        setLimits["identifierThresholdMS"] = "10";
        setLimits["commandThresholdMS"] = "0";
        setLimits["blockDurationMS"] = "60000";
        leader.executeWaitVerifyContent(setLimits, "200", true);

        // Force conflicts so commands escalate to the blocking queue and run on worker 0, which is what
        // accumulates per-identifier time.
        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        leader.executeWaitVerifyContent(setConflict, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingIdentifierThresholdMS"], "10");

        atomic<int> count503(0);
        atomic<int> count200(0);
        list<thread> threads;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &count503, &count200]() {
                BedrockTester& node = tester->getTester(i);
                vector<SData> requests;
                for (int j = 0; j < 200; j++) {
                    SData cmd("idcollision b4");
                    cmd["blockingQueueRateLimitIdentifier"] = "timeuser";
                    cmd["value"] = "node" + to_string(i) + "-" + to_string(j);
                    requests.push_back(cmd);
                }
                auto results = node.executeWaitMultipleData(requests);
                for (auto& result : results) {
                    int status = SToInt(result.methodLine);
                    if (status == 503) {
                        count503.fetch_add(1);
                    } else if (status == 200) {
                        count200.fetch_add(1);
                    }
                }
            });
        }
        for (thread& t : threads) {
            t.join();
        }

        ASSERT_EQUAL(count200.load() + count503.load(), 600);

        // Enforcement is happening, so we should see some 503s.
        ASSERT_TRUE(count503.load() > 0);

        // The identifier must register as blocked in Status.
        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_TRUE(SContains(json["blockingBlockedIdentifiers"], "timeuser"));

        SData clearBlocks("SetBlockingQueueTimeRateLimit");
        clearBlocks["ClearBlocks"] = "true";
        leader.executeWaitVerifyContent(clearBlocks, "200", true);

        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingBlockedIdentifiers"], "");

        // Reset leader state.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        SData resetLimit("SetBlockingQueueTimeRateLimit");
        resetLimit["identifierThresholdMS"] = "0";
        leader.executeWaitVerifyContent(resetLimit, "200", true);
    }

    void testGlobalRateLimiting()
    {
        BedrockTester& leader = tester->getTester(0);

        // Disable both per-sender dimensions so only the global one can reject anything, and give it a tiny
        // threshold so a burst of conflicting commands saturates the blocking thread.
        SData setLimits("SetBlockingQueueTimeRateLimit");
        setLimits["identifierThresholdMS"] = "0";
        setLimits["commandThresholdMS"] = "0";
        setLimits["globalWindowMS"] = "180000";
        setLimits["globalThresholdMS"] = "10";
        setLimits["globalBlockDurationMS"] = "60000";
        leader.executeWaitVerifyContent(setLimits, "200", true);

        // Force conflicts so commands escalate to the blocking queue and run on worker 0, which is what
        // accumulates the global time.
        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        leader.executeWaitVerifyContent(setConflict, "200", true);

        // Every command uses its own identifier, so neither per-sender dimension would ever see enough time to
        // trip. Only the global dimension can reject these.
        atomic<int> count503(0);
        atomic<int> count200(0);
        list<thread> threads;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &count503, &count200]() {
                BedrockTester& node = tester->getTester(i);
                vector<SData> requests;
                for (int j = 0; j < 200; j++) {
                    SData cmd("idcollision b4");
                    cmd["blockingQueueRateLimitIdentifier"] = "globaluser" + to_string(i) + "-" + to_string(j);
                    cmd["value"] = "node" + to_string(i) + "-" + to_string(j);
                    requests.push_back(cmd);
                }
                auto results = node.executeWaitMultipleData(requests);
                for (auto& result : results) {
                    int status = SToInt(result.methodLine);
                    if (status == 503) {
                        count503.fetch_add(1);
                    } else if (status == 200) {
                        count200.fetch_add(1);
                    }
                }
            });
        }
        for (thread& t : threads) {
            t.join();
        }

        ASSERT_EQUAL(count200.load() + count503.load(), 600);

        // Enforcement is happening, so we should see some 503s.
        ASSERT_TRUE(count503.load() > 0);

        // The global dimension did all the rejecting, so no identifier or command is blocked.
        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingGlobalBlocked"], "true");
        ASSERT_EQUAL(json["blockingBlockedIdentifiers"], "");
        ASSERT_EQUAL(json["blockingBlockedCommands"], "");

        SData clearBlocks("SetBlockingQueueTimeRateLimit");
        clearBlocks["ClearBlocks"] = "true";
        leader.executeWaitVerifyContent(clearBlocks, "200", true);

        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingGlobalBlocked"], "false");

        // Reset leader state.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        SData resetLimit("SetBlockingQueueTimeRateLimit");
        resetLimit["globalThresholdMS"] = "0";
        leader.executeWaitVerifyContent(resetLimit, "200", true);
    }
} __BlockingQueueRateLimitTest;
