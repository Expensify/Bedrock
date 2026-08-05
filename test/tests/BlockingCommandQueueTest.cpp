#include <BedrockBlockingCommandQueue.h>
#include <test/lib/tpunit++.hpp>

struct BlockingCommandQueueTest : tpunit::TestFixture
{
    BlockingCommandQueueTest() : tpunit::TestFixture("BlockingCommandQueue",
                                                     TEST(BlockingCommandQueueTest::testUnderLimitNotFlagged),
                                                     TEST(BlockingCommandQueueTest::testTimeAccumulatesAcrossCommands),
                                                     TEST(BlockingCommandQueueTest::testIdentifiersAreIndependent),
                                                     TEST(BlockingCommandQueueTest::testEmptyIdentifierNeverFlagged),
                                                     TEST(BlockingCommandQueueTest::testDisabledThresholdNeverFlags),
                                                     TEST(BlockingCommandQueueTest::testClearRateLimitsResets))
    {
    }

    void testUnderLimitNotFlagged()
    {
        BedrockBlockingCommandQueue queue;
        queue.setMaxTimePerIdentifier(10'000);

        ASSERT_FALSE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));

        queue.recordExecutionTime("acct1", 5'000);
        ASSERT_FALSE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));
    }

    void testTimeAccumulatesAcrossCommands()
    {
        // This is the burst case the dequeue check fixes: no single command is over the limit, but their accumulated
        // time is. The push-time check only sees time recorded before push, so re-checking at dequeue catches this.
        BedrockBlockingCommandQueue queue;
        queue.setMaxTimePerIdentifier(10'000);

        queue.recordExecutionTime("acct1", 6'000);
        ASSERT_FALSE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));

        queue.recordExecutionTime("acct1", 6'000);
        ASSERT_TRUE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));
    }

    void testIdentifiersAreIndependent()
    {
        BedrockBlockingCommandQueue queue;
        queue.setMaxTimePerIdentifier(10'000);

        queue.recordExecutionTime("acct1", 20'000);
        ASSERT_TRUE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));
        ASSERT_FALSE(queue.isIdentifierOverTimeLimit("acct2", "TestCommand"));
    }

    void testEmptyIdentifierNeverFlagged()
    {
        BedrockBlockingCommandQueue queue;
        queue.setMaxTimePerIdentifier(10'000);

        // An empty identifier means "no known account", so it is never rate limited.
        queue.recordExecutionTime("", 20'000);
        ASSERT_FALSE(queue.isIdentifierOverTimeLimit("", "TestCommand"));
    }

    void testDisabledThresholdNeverFlags()
    {
        BedrockBlockingCommandQueue queue;
        queue.setMaxTimePerIdentifier(10'000);

        queue.recordExecutionTime("acct1", 20'000);
        ASSERT_TRUE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));

        // A threshold of 0 disables time rate limiting entirely.
        queue.setMaxTimePerIdentifier(0);
        ASSERT_FALSE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));
    }

    void testClearRateLimitsResets()
    {
        BedrockBlockingCommandQueue queue;
        queue.setMaxTimePerIdentifier(10'000);

        queue.recordExecutionTime("acct1", 20'000);
        ASSERT_TRUE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));

        queue.clearRateLimits();
        ASSERT_FALSE(queue.isIdentifierOverTimeLimit("acct1", "TestCommand"));
    }
} __BlockingCommandQueueTest;
