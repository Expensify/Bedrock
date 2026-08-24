#include <BedrockBlockingCommandQueue.h>
#include <test/lib/tpunit++.hpp>

// A queue whose clock the test controls, so window and block behavior is deterministic. All times below are
// microseconds. The tests drive the public API and control the clock.
struct TestBlockingCommandQueue : public BedrockBlockingCommandQueue
{
    static unique_ptr<BedrockCommand> makeCommand(const string& identifier, const string& commandName)
    {
        SData request(commandName);
        auto command = make_unique<BedrockCommand>(SQLiteCommand(move(request)), nullptr);
        command->blockingQueueRateLimitIdentifier = identifier;
        return command;
    }

    void setNow(uint64_t now)
    {
        _testNow = now;
    }

protected:
    uint64_t _now() const override
    {
        return _testNow.load();
    }

private:
    atomic<uint64_t> _testNow{0};
};

struct BlockingCommandQueueTest : tpunit::TestFixture
{
    BlockingCommandQueueTest() : tpunit::TestFixture("BlockingCommandQueue",
                                                     TEST(BlockingCommandQueueTest::testIdentifierOverThresholdBlocks),
                                                     TEST(BlockingCommandQueueTest::testUnderThresholdNotBlocked),
                                                     TEST(BlockingCommandQueueTest::testIdentifiersAreIndependent),
                                                     TEST(BlockingCommandQueueTest::testCommandDimensionIgnoresIdentifier),
                                                     TEST(BlockingCommandQueueTest::testPushReportsRateLimitDimensions),
                                                     TEST(BlockingCommandQueueTest::testDequeueReportsRateLimitDimensions),
                                                     TEST(BlockingCommandQueueTest::testEmptyIdentifierSkipsIdentifierDimension),
                                                     TEST(BlockingCommandQueueTest::testWindowExpiry),
                                                     TEST(BlockingCommandQueueTest::testPartialCredit),
                                                     TEST(BlockingCommandQueueTest::testBlockDurationHoldsThenClears),
                                                     TEST(BlockingCommandQueueTest::testDisabledThresholdsNeverBlock),
                                                     TEST(BlockingCommandQueueTest::testClearResets))
    {
    }

    void testIdentifierOverThresholdBlocks()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setBlockDuration(1000);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 30);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");

        queue.recordExecutionTime("acct1", "cmd", 30);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "identifier");
    }

    void testUnderThresholdNotBlocked()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 40);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");
    }

    void testIdentifiersAreIndependent()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "identifier");
        ASSERT_EQUAL(queue._getBlockingDimension("acct2", "cmd"), "");
    }

    void testCommandDimensionIgnoresIdentifier()
    {
        // With the identifier dimension disabled, a command over its threshold blocks for every identifier.
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(0);
        queue.setCommandThreshold(50);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "command");
        ASSERT_EQUAL(queue._getBlockingDimension("acct2", "cmd"), "command");
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "otherCmd"), "");
    }

    void testPushReportsRateLimitDimensions()
    {
        TestBlockingCommandQueue identifierQueue;
        identifierQueue.setIdentifierThreshold(50);
        identifierQueue.setCommandThreshold(0);
        identifierQueue.setNow(1000);
        identifierQueue.recordExecutionTime("acct1", "cmd", 60);

        bool identifierRejected = false;
        try {
            identifierQueue.push(identifierQueue.makeCommand("acct1", "cmd"));
        } catch (const SException& e) {
            identifierRejected = true;
            ASSERT_EQUAL(string(e.what()), "503 Blocking queue rate limited (identifier)");
        }
        ASSERT_TRUE(identifierRejected);

        TestBlockingCommandQueue commandQueue;
        commandQueue.setIdentifierThreshold(0);
        commandQueue.setCommandThreshold(50);
        commandQueue.setNow(1000);
        commandQueue.recordExecutionTime("acct1", "cmd", 60);

        bool commandRejected = false;
        try {
            commandQueue.push(commandQueue.makeCommand("acct2", "cmd"));
        } catch (const SException& e) {
            commandRejected = true;
            ASSERT_EQUAL(string(e.what()), "503 Blocking queue rate limited (command)");
        }
        ASSERT_TRUE(commandRejected);

        TestBlockingCommandQueue bothDimensionsQueue;
        bothDimensionsQueue.setIdentifierThreshold(50);
        bothDimensionsQueue.setCommandThreshold(50);
        bothDimensionsQueue.setNow(1000);
        bothDimensionsQueue.recordExecutionTime("acct1", "cmd", 60);

        bool bothDimensionsRejected = false;
        try {
            bothDimensionsQueue.push(bothDimensionsQueue.makeCommand("acct1", "cmd"));
        } catch (const SException& e) {
            bothDimensionsRejected = true;
            ASSERT_EQUAL(string(e.what()), "503 Blocking queue rate limited (identifier)");
        }
        ASSERT_TRUE(bothDimensionsRejected);
    }

    void testDequeueReportsRateLimitDimensions()
    {
        TestBlockingCommandQueue identifierQueue;
        identifierQueue.setIdentifierThreshold(50);
        identifierQueue.setCommandThreshold(0);
        identifierQueue.setNow(1000);
        identifierQueue.push(identifierQueue.makeCommand("acct1", "cmd"));
        identifierQueue.recordExecutionTime("acct1", "cmd", 60);

        auto identifierCommand = identifierQueue.get(1'000'000);
        ASSERT_TRUE(identifierCommand->complete);
        ASSERT_EQUAL(identifierCommand->response.methodLine, "503 Blocking queue rate limited (identifier)");

        TestBlockingCommandQueue commandQueue;
        commandQueue.setIdentifierThreshold(0);
        commandQueue.setCommandThreshold(50);
        commandQueue.setNow(1000);
        commandQueue.push(commandQueue.makeCommand("acct2", "cmd"));
        commandQueue.recordExecutionTime("acct1", "cmd", 60);

        auto command = commandQueue.get(1'000'000);
        ASSERT_TRUE(command->complete);
        ASSERT_EQUAL(command->response.methodLine, "503 Blocking queue rate limited (command)");
    }

    void testEmptyIdentifierSkipsIdentifierDimension()
    {
        // An empty identifier is skipped, but the command dimension still applies.
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(50);
        queue.setNow(1000);

        queue.recordExecutionTime("", "cmd", 60);
        ASSERT_EQUAL(queue._getBlockingDimension("", "cmd"), "command");
    }

    void testWindowExpiry()
    {
        // A sample older than the window no longer counts toward the threshold.
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setBlockDuration(1000);
        queue.setNow(1000);

        // One 40us sample, under the 50us threshold on its own.
        queue.recordExecutionTime("acct1", "cmd", 40);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");

        // A full window later, record another 40us. The first has aged out, so the window holds only 40us
        // (< 50). If it still counted, the two would sum to 80 and block.
        queue.setNow(1150);
        queue.recordExecutionTime("acct1", "cmd", 40);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");
    }

    void testPartialCredit()
    {
        // A sample counts only for the part that still lies inside the window.
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(35);
        queue.setCommandThreshold(0);
        queue.setBlockDuration(1000);
        queue.setNow(1000);

        // 30us sample, under the 35us threshold.
        queue.recordExecutionTime("acct1", "cmd", 30);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");

        // 80us later, only window - age = 100 - 80 = 20us of the first sample still counts. With a new 10us
        // sample the window holds 20 + 10 = 30us (< 35). Counting the full 30 + 10 = 40 would block.
        queue.setNow(1080);
        queue.recordExecutionTime("acct1", "cmd", 10);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");
    }

    void testBlockDurationHoldsThenClears()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setBlockDuration(500);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "identifier");

        // The sample has aged out of the window, but the block still holds for its fixed duration.
        queue.setNow(1200);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "identifier");

        // After the block duration, with nothing left in the window, it clears.
        queue.setNow(1600);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");
    }

    void testDisabledThresholdsNeverBlock()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(0);
        queue.setCommandThreshold(0);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 1000000);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");
    }

    void testClearResets()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setBlockDuration(1000);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "identifier");

        queue.clearRateLimits();
        ASSERT_EQUAL(queue._getBlockingDimension("acct1", "cmd"), "");
    }
} __BlockingCommandQueueTest;
