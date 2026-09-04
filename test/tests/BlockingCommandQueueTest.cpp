#include <BedrockBlockingCommandQueue.h>
#include <test/lib/tpunit++.hpp>

// A queue whose clock the test controls, so window and block behavior is deterministic. All times below are
// microseconds. The tests drive the public API and control the clock.
struct TestBlockingCommandQueue : public BedrockBlockingCommandQueue
{
    static unique_ptr<BedrockCommand> makeCommand(const string& identifier, const string& commandName, const string& requestID = "", const string& logParam = "")
    {
        SData request(commandName);
        if (!requestID.empty()) {
            request["requestID"] = requestID;
        }
        if (!logParam.empty()) {
            request["logParam"] = logParam;
        }
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
                                                     TEST(BlockingCommandQueueTest::testDequeueRestoresLogPrefix),
                                                     TEST(BlockingCommandQueueTest::testEmptyIdentifierSkipsIdentifierDimension),
                                                     TEST(BlockingCommandQueueTest::testWindowExpiry),
                                                     TEST(BlockingCommandQueueTest::testPartialCredit),
                                                     TEST(BlockingCommandQueueTest::testBlockDurationHoldsThenClears),
                                                     TEST(BlockingCommandQueueTest::testDisabledThresholdsNeverBlock),
                                                     TEST(BlockingCommandQueueTest::testClearResets),
                                                     TEST(BlockingCommandQueueTest::testGlobalOverThresholdBlocksEveryone),
                                                     TEST(BlockingCommandQueueTest::testGlobalRejectsOnPushAndDequeue),
                                                     TEST(BlockingCommandQueueTest::testGlobalBlockTakesPrecedence),
                                                     TEST(BlockingCommandQueueTest::testGlobalWindowIsIndependent),
                                                     TEST(BlockingCommandQueueTest::testGlobalBlockDurationHoldsThenClears),
                                                     TEST(BlockingCommandQueueTest::testClearResetsGlobalBlock))
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
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");

        queue.recordExecutionTime("acct1", "cmd", 30);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "identifier");
    }

    void testUnderThresholdNotBlocked()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 40);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
    }

    void testIdentifiersAreIndependent()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "identifier");
        ASSERT_EQUAL(queue.getBlockingDimension("acct2", "cmd"), "");
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
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "command");
        ASSERT_EQUAL(queue.getBlockingDimension("acct2", "cmd"), "command");
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "otherCmd"), "");
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

    void testDequeueRestoresLogPrefix()
    {
        // `_dequeue()`'s prefix is scoped, so it must be gone once `get()` returns: the blocking worker sets its own
        // prefix after `get()` and would otherwise inherit a stale one.
        TestBlockingCommandQueue queue;
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setNow(1000);
        queue.push(queue.makeCommand("acct1", "cmd", "rejected1", "rejected@example.com"));
        queue.recordExecutionTime("acct1", "cmd", 60);

        SData outerRequest("outerCmd");
        outerRequest["requestID"] = "outer1";
        outerRequest["logParam"] = "outer@example.com";
        SAUTOPREFIX(outerRequest);

        auto command = queue.get(1'000'000);
        ASSERT_TRUE(command->complete);
        ASSERT_EQUAL(command->response.methodLine, "503 Blocking queue rate limited (identifier)");

        ASSERT_EQUAL(SThreadLogPrefix, "outer1");
        ASSERT_EQUAL(SThreadLogParam, "outer@example.com");
        ASSERT_EQUAL(SThreadLogCommand, "outerCmd");
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
        ASSERT_EQUAL(queue.getBlockingDimension("", "cmd"), "command");
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
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");

        // A full window later, record another 40us. The first has aged out, so the window holds only 40us
        // (< 50). If it still counted, the two would sum to 80 and block.
        queue.setNow(1150);
        queue.recordExecutionTime("acct1", "cmd", 40);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
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
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");

        // 80us later, only window - age = 100 - 80 = 20us of the first sample still counts. With a new 10us
        // sample the window holds 20 + 10 = 30us (< 35). Counting the full 30 + 10 = 40 would block.
        queue.setNow(1080);
        queue.recordExecutionTime("acct1", "cmd", 10);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
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
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "identifier");

        // The sample has aged out of the window, but the block still holds for its fixed duration.
        queue.setNow(1200);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "identifier");

        // After the block duration, with nothing left in the window, it clears.
        queue.setNow(1600);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
    }

    void testDisabledThresholdsNeverBlock()
    {
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(0);
        queue.setCommandThreshold(0);
        queue.setGlobalThreshold(0);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 1000000);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
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
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "identifier");

        queue.clearRateLimits();
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
    }

    void testGlobalOverThresholdBlocksEveryone()
    {
        // The global dimension counts every command, so a burst spread over several identifiers trips it even
        // though no single identifier or command is over a limit of its own.
        TestBlockingCommandQueue queue;
        queue.setIdentifierThreshold(0);
        queue.setCommandThreshold(0);
        queue.setGlobalWindow(100);
        queue.setGlobalThreshold(50);
        queue.setGlobalBlockDuration(1000);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd1", 30);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd1"), "");

        queue.recordExecutionTime("acct2", "cmd2", 30);
        ASSERT_EQUAL(queue.getBlockingDimension("acct2", "cmd2"), "global");
        ASSERT_EQUAL(queue.getBlockingDimension("acct3", "cmd3"), "global");
        ASSERT_EQUAL(queue.getBlockingDimension("", ""), "global");
    }

    void testGlobalRejectsOnPushAndDequeue()
    {
        TestBlockingCommandQueue pushQueue;
        pushQueue.setIdentifierThreshold(0);
        pushQueue.setCommandThreshold(0);
        pushQueue.setGlobalThreshold(50);
        pushQueue.setNow(1000);
        pushQueue.recordExecutionTime("acct1", "cmd", 60);

        bool pushRejected = false;
        try {
            pushQueue.push(pushQueue.makeCommand("acct2", "otherCmd"));
        } catch (const SException& e) {
            pushRejected = true;
            ASSERT_EQUAL(string(e.what()), "503 Blocking queue rate limited (global)");
        }
        ASSERT_TRUE(pushRejected);

        // Commands that were already queued are rejected on the way out, so a backlog drains instead of running.
        TestBlockingCommandQueue dequeueQueue;
        dequeueQueue.setIdentifierThreshold(0);
        dequeueQueue.setCommandThreshold(0);
        dequeueQueue.setGlobalThreshold(50);
        dequeueQueue.setNow(1000);
        dequeueQueue.push(dequeueQueue.makeCommand("acct2", "otherCmd"));
        dequeueQueue.recordExecutionTime("acct1", "cmd", 60);

        auto command = dequeueQueue.get(1'000'000);
        ASSERT_TRUE(command->complete);
        ASSERT_EQUAL(command->response.methodLine, "503 Blocking queue rate limited (global)");
    }

    void testGlobalBlockTakesPrecedence()
    {
        // One command can trip the identifier and the global dimension at once. The reject reports global,
        // because that is the one that also rejects everybody else.
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(50);
        queue.setCommandThreshold(0);
        queue.setBlockDuration(1000);
        queue.setGlobalWindow(100);
        queue.setGlobalThreshold(50);
        queue.setGlobalBlockDuration(1000);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "global");
    }

    void testGlobalWindowIsIndependent()
    {
        // The global dimension has its own window, so a sample that has aged out of the shared window can still
        // count toward the global threshold.
        TestBlockingCommandQueue queue;
        queue.setWindow(100);
        queue.setIdentifierThreshold(0);
        queue.setCommandThreshold(0);
        queue.setGlobalWindow(1000);
        queue.setGlobalThreshold(50);
        queue.setGlobalBlockDuration(1000);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 30);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");

        // 500us later the first sample is far outside the 100us shared window but inside the 1000us global
        // window, so all 30us of it still counts and the second sample takes the total to 60 (> 50).
        queue.setNow(1500);
        queue.recordExecutionTime("acct1", "cmd", 30);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "global");
    }

    void testGlobalBlockDurationHoldsThenClears()
    {
        TestBlockingCommandQueue queue;
        queue.setIdentifierThreshold(0);
        queue.setCommandThreshold(0);
        queue.setGlobalWindow(100);
        queue.setGlobalThreshold(50);
        queue.setGlobalBlockDuration(500);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "global");

        // The sample has aged out of the window, but the block still holds for its fixed duration.
        queue.setNow(1200);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "global");

        queue.setNow(1600);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
    }

    void testClearResetsGlobalBlock()
    {
        TestBlockingCommandQueue queue;
        queue.setIdentifierThreshold(0);
        queue.setCommandThreshold(0);
        queue.setGlobalWindow(100);
        queue.setGlobalThreshold(50);
        queue.setGlobalBlockDuration(1000);
        queue.setNow(1000);

        queue.recordExecutionTime("acct1", "cmd", 60);
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "global");

        queue.clearRateLimits();
        ASSERT_EQUAL(queue.getBlockingDimension("acct1", "cmd"), "");
    }
} __BlockingCommandQueueTest;
