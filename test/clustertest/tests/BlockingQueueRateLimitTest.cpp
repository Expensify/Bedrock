#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct BlockingQueueRateLimitTest : tpunit::TestFixture
{
    BlockingQueueRateLimitTest()
        : tpunit::TestFixture("BlockingQueueRateLimit",
                              BEFORE_CLASS(BlockingQueueRateLimitTest::setup),
                              BEFORE(BlockingQueueRateLimitTest::before),
                              TEST(BlockingQueueRateLimitTest::testControlCommands),
                              TEST(BlockingQueueRateLimitTest::testRateLimiting),
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

    void before() {
        BedrockTester& leader = tester->getTester(0);

        // ClearBlocks should reset all identifier counts.
        SData resetBlockingQueue("SetBlockingQueueRateLimit");
        resetBlockingQueue["ClearBlocks"] = "true";
        resetBlockingQueue["MaxRequestsPerIdentifier"] = "0";
        leader.executeWaitVerifyContent(resetBlockingQueue, "200", true);
        
        // Reset state on leader.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockedIdentifiers"], "0");
    }

    void testControlCommands()
    {
        BedrockTester& leader = tester->getTester(0);

        // Set the blocking rate limit threshold and verify it shows up in Status.
        SData setLimit("SetBlockingQueueRateLimit");
        setLimit["MaxRequestsPerIdentifier"] = "5";
        leader.executeWaitVerifyContent(setLimit, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingRateLimitThreshold"], "5");

        SData setBlockingQueueRateLimit("SetBlockingQueueRateLimit");
        setBlockingQueueRateLimit["MaxRequestsPerIdentifier"] = "1";
        leader.executeWaitVerifyContent(setBlockingQueueRateLimit, "200", true);
        
        SData setBlockingQueueTimeRateLimit("SetBlockingQueueTimeRateLimit");
        setBlockingQueueTimeRateLimit["MaxTimePerIdentifierMs"] = "1";
        leader.executeWaitVerifyContent(setBlockingQueueTimeRateLimit, "200", true);

        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingTimeRateLimitThresholdMs"], "1");
        ASSERT_EQUAL(json["blockingRateLimitThreshold"], "1");
    }

    void testRateLimiting()
    {
        // Only the leader processes writes (followers escalate), so only the leader needs these settings.
        BedrockTester& leader = tester->getTester(0);

        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        leader.executeWaitVerifyContent(setConflict, "200", true);

        SData setLimit("SetBlockingQueueRateLimit");
        setLimit["MaxRequestsPerIdentifier"] = "2";
        leader.executeWaitVerifyContent(setLimit, "200", true);

        // Remove the time limit so we're sure we're testing just the count limit.
        SData setBlockingQueueTimeRateLimit("SetBlockingQueueTimeRateLimit");
        setBlockingQueueTimeRateLimit["MaxTimePerIdentifierMs"] = "0";
        leader.executeWaitVerifyContent(setBlockingQueueTimeRateLimit, "200", true);

        // Spawn 3 threads, each sending 200 idcollision commands to a different node,
        // all with the same blockingQueueRateLimitIdentifier. This generates write conflicts that push
        // commands into the blocking queue, triggering rate limiting.
        atomic<int> count503(0);
        atomic<int> count200(0);
        list<thread> threads;

        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &count503, &count200]() {
                BedrockTester& node = tester->getTester(i);

                vector<SData> requests;
                for (int j = 0; j < 200; j++) {
                    SData cmd("idcollision b2");
                    cmd["blockingQueueRateLimitIdentifier"] = "testuser";
                    cmd["writeConsistency"] = "ASYNC";
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
        
        // We're not rejecting for counts right now, so this should return 0
        ASSERT_EQUAL(count503.load(), 0);
    }

    void testTimeRateLimiting()
    {
        BedrockTester& leader = tester->getTester(0);

        // Remove the count limit so we're sure we're testing just the time limit.
        SData setLimit("SetBlockingQueueRateLimit");
        setLimit["MaxRequestsPerIdentifier"] = "0";
        leader.executeWaitVerifyContent(setLimit, "200", true);

        // Set the time limit to 10ms so that we can trigger it with a few commands.
        SData setBlockingQueueTimeRateLimit("SetBlockingQueueTimeRateLimit");
        setBlockingQueueTimeRateLimit["MaxTimePerIdentifierMs"] = "10";
        leader.executeWaitVerifyContent(setBlockingQueueTimeRateLimit, "200", true);
        
        // Force conflicts so commands escalate to the blocking queue and run on worker 0,
        // which is what accumulates per-identifier time.
        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        leader.executeWaitVerifyContent(setConflict, "200", true);
        
        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingTimeRateLimitThresholdMs"], "10");

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
        // Confirm all requests finished
        ASSERT_EQUAL(count200.load() + count503.load(), 600);
        
        // Enforcement is now happening, so we should see some 503s.
        ASSERT_TRUE(count503.load() > 0);

        // After traffic, the identifier's accumulated time should be visible in Status, and with the
        // threshold at 1ms the active identifier must register as over the time limit.
        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_TRUE(json.find("blockingQueueIdentifierTimesMs") != json.end());
        ASSERT_TRUE(SToInt(json["blockedTimeIdentifiers"]) >= 1);

        // ClearBlocks resets time and count state together.
        SData clearBlocks("SetBlockingQueueTimeRateLimit");
        clearBlocks["ClearBlocks"] = "true";
        leader.executeWaitVerifyContent(clearBlocks, "200", true);

        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockedTimeIdentifiers"], "0");

        // Reset leader state.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        SData resetLimit("SetBlockingQueueTimeRateLimit");
        resetLimit["MaxTimePerIdentifierMs"] = "0";
        leader.executeWaitVerifyContent(resetLimit, "200", true);
    }
} __BlockingQueueRateLimitTest;
