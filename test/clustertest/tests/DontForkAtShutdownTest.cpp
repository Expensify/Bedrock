#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct DontForkAtShutdownTest : tpunit::TestFixture {
    DontForkAtShutdownTest()
        : tpunit::TestFixture("DontForkAtShutdown", TEST(DontForkAtShutdownTest::test)) {}

    void test() {
        // Create a cluster, wait for it to come up.
        BedrockClusterTester tester(ClusterSize::THREE_NODE_CLUSTER, {}, {
            {"-gracefulShutdownTimeoutSec", "0"}
        });

        // We'll tell the threads to stop when they're done.
        atomic<bool> stop(false);

        // We want to not spam a stopped leader.
        atomic<bool> leaderIsUp(true);
        
        // Just use a bunch of copies of the same command.
        SData spamCommand("randominsert");

        // In a vector.
        const vector<SData> commands(100, spamCommand);

        // Now create 9 threads spamming 100 commands at a time, each. 9 cause we have three nodes.
        vector<thread> threads;
        for (size_t i = 0; i < 9; i++) {
            threads.emplace_back([&tester, i, &commands, &stop, &leaderIsUp](){
                while (!stop) {
                    // Pick a tester, send, don't care about the result.
                    size_t testerNum = i % 3;
                    if (testerNum == 0 && !leaderIsUp) {
                        // If we're looking for leader and it's down, wait a second to avoid pegging the CPU.
                        sleep(1);
                    } else {
                        // If we're not leader or leader is up, spam away!
                        tester.getTester(testerNum).executeWaitMultipleData(commands);
                    }
                }
            });
        }

        // Ok, we're spamming a lot of commands. We need to block the sync thread for long enough that our shutdown can time out.
        thread quorumCommand([&tester](){
            // This command generaly takes 5-8 seconds, ish, depending on where you run it.
            SData slowQuery("slowprocessquery");
            slowQuery["size"] = "1000";
            slowQuery["count"] = "1000";
            slowQuery["writeConsistency"] = "2"; // QUORUM.
            tester.getTester(0).executeWaitMultipleData({slowQuery});
        });

        // Now that the above command is attempting to run, we wait 1 second for it to actually start, in case it's sitting queued for a few ms.
        sleep(1);

        // It *should be* running by now. We can try and stop the leader.
        leaderIsUp = false;
        tester.getTester(0).stopServer();

        // Done with the above thread, wait for it to be complete.
        quorumCommand.join();

        // Start leader again. (don't wait for it to come up all the way).
        tester.getTester(0).startServer(false);

        // Wait for it to be leading. If this works, it didn't fork.
        ASSERT_TRUE(tester.getTester(0).waitForState("LEADING"));

        // Done.
        stop = true;

        // Clean up.
        for (auto& t : threads) {
            t.join();
        }
    }
} __DontForkAtShutdownTest;
