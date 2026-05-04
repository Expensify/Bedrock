#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct BlockingQueueRateLimitTest : tpunit::TestFixture
{
    BlockingQueueRateLimitTest()
        : tpunit::TestFixture("BlockingQueueRateLimit",
                              BEFORE_CLASS(BlockingQueueRateLimitTest::setup),
                              TEST(BlockingQueueRateLimitTest::testControlCommands),
                              TEST(BlockingQueueRateLimitTest::testRateLimiting),
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

    void testControlCommands()
    {
        BedrockTester& leader = tester->getTester(0);

        // Set the blocking rate limit threshold and verify it shows up in Status.
        SData setLimit("SetBlockingRateLimit");
        setLimit["MaxPerIdentifier"] = "5";
        leader.executeWaitVerifyContent(setLimit, "200", true);

        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockingRateLimitThreshold"], "5");

        // Force conflicts to push commands into the blocking queue and trigger rate limiting.
        // Only the leader processes writes (followers escalate), so only the leader needs these settings.
        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        leader.executeWaitVerifyContent(setConflict, "200", true);

        SData setLimitLow("SetBlockingRateLimit");
        setLimitLow["MaxPerIdentifier"] = "1";
        leader.executeWaitVerifyContent(setLimitLow, "200", true);

        // Send commands from multiple threads to all nodes. Cross-node escalation generates
        // enough concurrent writes to reliably pile up 2+ commands in the blocking queue.
        list<thread> threads;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i]() {
                BedrockTester& node = tester->getTester(i);
                vector<SData> requests;
                for (int j = 0; j < 200; j++) {
                    SData cmd("idcollision");
                    cmd["blockingIdentifier"] = "controltest";
                    cmd["value"] = to_string(i * 200 + j);
                    requests.push_back(cmd);
                }
                node.executeWaitMultipleData(requests);
            });
        }
        for (thread& t : threads) {
            t.join();
        }

        // At least one identifier should now be blocked.
        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_TRUE(SToInt(json["blockedIdentifiers"]) >= 1);

        // ClearBlocks should remove all blocked identifiers.
        SData clearBlocks("SetBlockingRateLimit");
        clearBlocks["ClearBlocks"] = "true";
        leader.executeWaitVerifyContent(clearBlocks, "200", true);

        json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        ASSERT_EQUAL(json["blockedIdentifiers"], "0");

        // Reset state on leader.
        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        SData resetLimit("SetBlockingRateLimit");
        resetLimit["MaxPerIdentifier"] = "0";
        leader.executeWaitVerifyContent(resetLimit, "200", true);
    }

    void testRateLimiting()
    {
        // Only the leader processes writes (followers escalate), so only the leader needs these settings.
        BedrockTester& leader = tester->getTester(0);

        SData setConflict("SetConflictParams");
        setConflict["MaxConflictRetries"] = "1";
        leader.executeWaitVerifyContent(setConflict, "200", true);

        SData setLimit("SetBlockingRateLimit");
        setLimit["MaxPerIdentifier"] = "2";
        leader.executeWaitVerifyContent(setLimit, "200", true);

        // Spawn 3 threads, each sending 200 idcollision commands to a different node,
        // all with the same blockingIdentifier. This generates write conflicts that push
        // commands into the blocking queue, triggering rate limiting.
        atomic<int> count503(0);
        atomic<int> count200(0);
        list<thread> threads;

        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &count503, &count200]() {
                BedrockTester& node = tester->getTester(i);

                vector<SData> requests;
                for (int j = 0; j < 200; j++) {
                    SData cmd("idcollision");
                    cmd["blockingIdentifier"] = "testuser";
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
        ASSERT_TRUE(count503.load() >= 1);

        // Verify the leader shows blocked users.
        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        int blockedCount = SToInt(json["blockedIdentifiers"]);
        ASSERT_TRUE(blockedCount >= 1);

        // Clear blocks and reset on leader.
        SData clearBlocks("SetBlockingRateLimit");
        clearBlocks["ClearBlocks"] = "true";
        leader.executeWaitVerifyContent(clearBlocks, "200", true);

        SData resetConflict("SetConflictParams");
        resetConflict["MaxConflictRetries"] = "3";
        leader.executeWaitVerifyContent(resetConflict, "200", true);

        // Verify the previously blocked user can send commands again.
        SData cmd("idcollision");
        cmd["blockingIdentifier"] = "testuser";
        leader.executeWaitVerifyContent(cmd, "200");
    }
};
// Disabled while rate limiting is log-only. Re-enable enforcement (the STHROW in
// BedrockBlockingCommandQueue::push) and restore the static instance below to run these tests:
// __BlockingQueueRateLimitTest;
