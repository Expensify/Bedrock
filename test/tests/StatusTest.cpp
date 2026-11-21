#include <BedrockCommandQueue.h>
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct StatusTest : tpunit::TestFixture
{
    StatusTest()
        : tpunit::TestFixture("Status",
                              TEST(StatusTest::test),
                              TEST(StatusTest::testCommandCountsExcludeOnlyFutureSchedules),
                              TEST(StatusTest::testFutureCommandsRemainQueued))
    {
    }

    void test()
    {
        BedrockTester tester;
        SData status("Status");
        string response = tester.executeWaitMultipleData({status})[0].content;
        ASSERT_TRUE(SContains(response, "plugins"));
        ASSERT_TRUE(SContains(response, "outstandingFramesToCheckpoint"));
        ASSERT_TRUE(SContains(response, "freelistCount"));
        ASSERT_TRUE(SContains(response, "pageCount"));
        ASSERT_TRUE(tester.waitForStatusTerm("activeCommandCount", "1"));
    }

    void testCommandCountsExcludeOnlyFutureSchedules()
    {
        BedrockCommandQueue queue;
        const uint64_t now = STimeNow();
        const uint64_t futureTime = now + STIME_US_PER_D;

        SData futureRequest("future");
        futureRequest["commandExecuteTime"] = to_string(futureTime);
        queue.push(make_unique<BedrockCommand>(SQLiteCommand(SData(futureRequest)), nullptr));

        SData dueRequest("due");
        dueRequest["commandExecuteTime"] = to_string(now - 1);
        queue.push(make_unique<BedrockCommand>(SQLiteCommand(SData(dueRequest)), nullptr));

        // A conflict retry changes its queue time, not the command's original schedule.
        dueRequest.methodLine = "retry";
        queue.push(make_unique<BedrockCommand>(SQLiteCommand(move(dueRequest)), nullptr), futureTime);

        // An expired deadline makes even a future schedule ready for timeout handling.
        futureRequest.methodLine = "expired";
        futureRequest["timeout"] = to_string(-2 * static_cast<int64_t>(STIME_US_PER_D / STIME_US_PER_MS));
        queue.push(make_unique<BedrockCommand>(SQLiteCommand(move(futureRequest)), nullptr));

        const auto counts = queue.getCommandCounts();
        ASSERT_EQUAL(counts.commandCount - counts.activeCommandCount, 1);
        ASSERT_EQUAL(queue.size(), 4);

        auto expiredCommand = queue.get(1);
        ASSERT_EQUAL(expiredCommand->request.methodLine, "expired");
        auto dueCommand = queue.get(1);
        ASSERT_EQUAL(dueCommand->request.methodLine, "due");
        ASSERT_EQUAL(queue.size(), 2);

        // Reading the counts leaves the scheduled work in the queue.
        const auto remainingCounts = queue.getCommandCounts();
        ASSERT_EQUAL(remainingCounts.commandCount - remainingCounts.activeCommandCount, 1);
        queue.clear();
        const auto emptyCounts = queue.getCommandCounts();
        ASSERT_EQUAL(emptyCounts.commandCount, emptyCounts.activeCommandCount);
    }

    void testFutureCommandsRemainQueued()
    {
        // Given an insert scheduled for tomorrow.
        BedrockTester tester;
        SData query("Query");
        query["query"] = "CREATE TABLE scheduledStatusTest (value INTEGER);";
        tester.executeWaitVerifyContent(query);

        query["query"] = "INSERT INTO scheduledStatusTest VALUES (1);";
        query["commandExecuteTime"] = to_string(STimeNow() + STIME_US_PER_D);
        tester.executeWaitVerifyContent(query, "202");

        // When waiting for immediate work to finish.
        ASSERT_TRUE(tester.waitForStatusTerm("commandCount", "2"));
        ASSERT_TRUE(tester.waitForStatusTerm("activeCommandCount", "1"));

        // Then the future insert must stay queued and unexecuted.
        ASSERT_FALSE(tester.waitForStatusTerm("commandCount", "1", 100'000));
        ASSERT_EQUAL(tester.readDB("SELECT COUNT(*) FROM scheduledStatusTest;"), "0");

        // HCTree reads may still be finishing, so compare counts from the same snapshot.
        const STable status = tester.executeWaitVerifyContentTable(SData("Status"));
        ASSERT_EQUAL(SToUInt64(status.at("commandCount")) - SToUInt64(status.at("activeCommandCount")), 1);
        ASSERT_EQUAL(status.at("queuedCommandList"), "[\"Query\"]");
    }
} __StatusTest;
