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

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingBlockedAccounts"], "");
    }

    void testControlCommands()
    {
        BedrockTester& leader = tester->getTester(0);

        // Set the window and thresholds and verify they show up in Status.
        SData setLimits("SetBlockingQueueTimeRateLimit");
        setLimits["WindowMS"] = "180000";
        setLimits["AccountThresholdMS"] = "20000";
        setLimits["CommandThresholdMS"] = "40000";
        setLimits["BlockDurationMS"] = "60000";
        leader.executeWaitVerifyContent(setLimits, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingTimeWindowMS"], "180000");
        ASSERT_EQUAL(json["blockingAccountThresholdMS"], "20000");
        ASSERT_EQUAL(json["blockingCommandThresholdMS"], "40000");
        ASSERT_EQUAL(json["blockingBlockDurationMS"], "60000");
    }

    void testTimeRateLimiting()
    {
        BedrockTester& leader = tester->getTester(0);

        // Small account threshold so a burst of conflicting commands trips it. The command dimension is
        // disabled so this isolates the account dimension.
        SData setLimits("SetBlockingQueueTimeRateLimit");
        setLimits["WindowMS"] = "180000";
        setLimits["AccountThresholdMS"] = "10";
        setLimits["CommandThresholdMS"] = "0";
        setLimits["BlockDurationMS"] = "60000";
        leader.executeWaitVerifyContent(setLimits, "200", true);

        // Force conflicts so commands escalate to the blocking queue and run on worker 0, which is what
        // accumulates per-account time.
        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        leader.executeWaitVerifyContent(setConflict, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingAccountThresholdMS"], "10");

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

        // The account must register as blocked in Status.
        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_TRUE(SContains(json["blockingBlockedAccounts"], "timeuser"));

        SData clearBlocks("SetBlockingQueueTimeRateLimit");
        clearBlocks["ClearBlocks"] = "true";
        leader.executeWaitVerifyContent(clearBlocks, "200", true);

        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingBlockedAccounts"], "");

        // Reset leader state.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        SData resetLimit("SetBlockingQueueTimeRateLimit");
        resetLimit["AccountThresholdMS"] = "0";
        leader.executeWaitVerifyContent(resetLimit, "200", true);
    }
} __BlockingQueueRateLimitTest;
