#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct StatusTest : tpunit::TestFixture
{
    StatusTest()
        : tpunit::TestFixture("Status", TEST(StatusTest::test), TEST(StatusTest::checkpointingKeepsPaceWithCommits))
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
    }

    // Checkpointing has to keep pace with commits. This does not reproduce the `wal2` hook suppression that let
    // checkpointing stop entirely in production, but it does fail if checkpointing stops for any reason: the backlog
    // would then grow with every write instead of cycling.
    void checkpointingKeepsPaceWithCommits()
    {
        BedrockTester tester({}, {"CREATE TABLE framecount (id INTEGER PRIMARY KEY, value BLOB);"});

        // Each of these commits roughly 52 WAL frames, so this writes on the order of 10,000 frames in total.
        const int writeCount = 200;
        for (int i = 0; i < writeCount; i++) {
            SData query("Query");
            query["query"] = "INSERT INTO framecount VALUES (" + to_string(i) + ", zeroblob(200000));";
            tester.executeWaitVerifyContent(query);
        }

        SData status("Status");
        STable response = SParseJSONObject(tester.executeWaitMultipleData({status})[0].content);
        const uint64_t outstandingFrames = SToUInt64(response["outstandingFramesToCheckpoint"]);

        // The backlog cycles as the WAL fills and switches, so it lands somewhere under a couple of thousand frames
        // rather than holding everything the test wrote.
        ASSERT_LESS_THAN(outstandingFrames, static_cast<uint64_t>(3000));
    }
} __StatusTest;
