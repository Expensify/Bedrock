#include <BedrockBlockingCommandQueue.h>
#include <test/lib/tpunit++.hpp>

// Unit tests for BedrockBlockingCommandQueue::isIdentifierOverTimeLimit, the check the blockingCommit worker runs at
// dequeue. These exercise the accumulator directly via recordExecutionTime, with no server, so they're deterministic.
struct BlockingCommandQueueTest : tpunit::TestFixture
{
    BlockingCommandQueueTest() : tpunit::TestFixture("BlockingCommandQueue",
                                                     TEST(BlockingCommandQueueTest::testUnderLimitNotFlagged))
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
} __BlockingCommandQueueTest;
