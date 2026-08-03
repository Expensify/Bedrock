#include <BedrockBlockingCommandQueue.h>
#include <test/lib/tpunit++.hpp>

// Unit tests for BedrockBlockingCommandQueue::isIdentifierOverTimeLimit, the check the blockingCommit worker runs at
// dequeue. These exercise the accumulator directly via recordExecutionTime, with no server, so they're deterministic.
struct BlockingCommandQueueTest : tpunit::TestFixture
{
    BlockingCommandQueueTest() : tpunit::TestFixture("BlockingCommandQueue",
                                                     TEST(BlockingCommandQueueTest::testUnderLimitNotFlagged),
                                                     TEST(BlockingCommandQueueTest::testTimeAccumulatesAcrossCommands),
                                                     TEST(BlockingCommandQueueTest::testIdentifiersAreIndependent))
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
} __BlockingCommandQueueTest;
