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
        // Set MaxConflictRetries=1 and MaxPerIdentifier=1 on all nodes so the 2nd command
        // for the same identifier triggers a block after one conflict retry.
        for (int i : {0, 1, 2}) {
            BedrockTester& node = tester->getTester(i);
            SData setConflict("SetConflictParams");
            setConflict["MaxConflictRetries"] = "1";
            node.executeWaitVerifyContent(setConflict, "200", true);

            SData setLimitLow("SetBlockingRateLimit");
            setLimitLow["MaxPerIdentifier"] = "1";
            node.executeWaitVerifyContent(setLimitLow, "200", true);
        }

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

        // Reset state on all nodes.
        for (int i : {0, 1, 2}) {
            BedrockTester& node = tester->getTester(i);
            SData resetConflict("SetConflictParams");
            resetConflict["MaxConflictRetries"] = "3";
            node.executeWaitVerifyContent(resetConflict, "200", true);

            SData resetLimit("SetBlockingRateLimit");
            resetLimit["MaxPerIdentifier"] = "0";
            node.executeWaitVerifyContent(resetLimit, "200", true);
        }
    }

    void testRateLimiting()
    {
        // Set MaxConflictRetries=1 on all nodes so commands enter the blocking queue after 1 conflict.
        for (int i : {0, 1, 2}) {
            BedrockTester& node = tester->getTester(i);
            SData setConflict("SetConflictParams");
            setConflict["MaxConflictRetries"] = "1";
            node.executeWaitVerifyContent(setConflict, "200", true);
        }

        // Set MaxPerIdentifier=2 on all nodes. Low threshold so rate limiting triggers quickly
        // once commands start entering the blocking queue.
        for (int i : {0, 1, 2}) {
            BedrockTester& node = tester->getTester(i);
            SData setLimit("SetBlockingRateLimit");
            setLimit["MaxPerIdentifier"] = "2";
            node.executeWaitVerifyContent(setLimit, "200", true);
        }

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
        BedrockTester& leader = tester->getTester(0);
        SData status("Status");
        STable json = SParseJSONObject(leader.executeWaitVerifyContent(status, "200", true));
        int blockedCount = SToInt(json["blockedIdentifiers"]);
        ASSERT_TRUE(blockedCount >= 1);

        // Clear blocks on all nodes.
        for (int i : {0, 1, 2}) {
            BedrockTester& node = tester->getTester(i);
            SData clearBlocks("SetBlockingRateLimit");
            clearBlocks["ClearBlocks"] = "true";
            node.executeWaitVerifyContent(clearBlocks, "200", true);
        }

        // Reset MaxConflictRetries back to default on all nodes.
        for (int i : {0, 1, 2}) {
            BedrockTester& node = tester->getTester(i);
            SData resetConflict("SetConflictParams");
            resetConflict["MaxConflictRetries"] = "3";
            node.executeWaitVerifyContent(resetConflict, "200", true);
        }

        // Verify the previously blocked user can send commands again.
        SData cmd("idcollision");
        cmd["blockingIdentifier"] = "testuser";
        leader.executeWaitVerifyContent(cmd, "200");
    }
} __BlockingQueueRateLimitTest;
