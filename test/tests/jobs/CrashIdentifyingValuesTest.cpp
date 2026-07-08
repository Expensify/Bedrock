#include <libstuff/SData.h>
#include <plugins/Jobs.h>
#include <test/lib/BedrockTester.h>

// Unit tests for BedrockJobsCommand::populateCrashIdentifyingValues(). The crash blacklist
// ("poison-pill" protection) refuses any future command whose methodLine and crashIdentifyingValues
// exactly match a command that previously crashed a node. If a volatile, per-request field such as
// `requestID` is included, the entry can never match a future command (no two requests share a
// requestID), making the protection a no-op. These tests verify those volatile fields are excluded
// while the semantically meaningful fields are retained.
struct JobsCrashIdentifyingValuesTest : tpunit::TestFixture
{
    JobsCrashIdentifyingValuesTest()
        : tpunit::TestFixture("JobsCrashIdentifyingValues",
                              TEST(JobsCrashIdentifyingValuesTest::excludesVolatileFields),
                              TEST(JobsCrashIdentifyingValuesTest::retainsAllFieldsWhenNoneAreVolatile))
    {
    }

    // Volatile per-request fields are dropped; meaningful fields (and their values) are kept.
    void excludesVolatileFields()
    {
        SData request("GetJob");
        request["name"] = "TestJob";
        request["data"] = "{\"key\":\"value\"}";
        request["jobID"] = "12345";
        request["requestID"] = "abc123";
        request["ID"] = "999";
        request["lastIP"] = "127.0.0.1";
        request["_source"] = "someSource";

        BedrockJobsCommand command(SQLiteCommand(move(request)), nullptr);
        command.populateCrashIdentifyingValues();

        // Volatile per-request fields must be excluded so the blacklist entry can actually match a
        // genuinely repeated command.
        ASSERT_FALSE(command.crashIdentifyingValues.count("requestID"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("ID"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("lastIP"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("_source"));

        // Semantically meaningful fields must be retained, with their original values.
        ASSERT_TRUE(command.crashIdentifyingValues.count("name"));
        ASSERT_TRUE(command.crashIdentifyingValues.count("data"));
        ASSERT_TRUE(command.crashIdentifyingValues.count("jobID"));
        ASSERT_EQUAL(command.crashIdentifyingValues["name"], "TestJob");
        ASSERT_EQUAL(command.crashIdentifyingValues["data"], "{\"key\":\"value\"}");
        ASSERT_EQUAL(command.crashIdentifyingValues["jobID"], "12345");

        // Only the three non-volatile fields should be present.
        ASSERT_EQUAL(command.crashIdentifyingValues.size(), 3u);
    }

    // A command with no volatile fields keeps every field.
    void retainsAllFieldsWhenNoneAreVolatile()
    {
        SData request("CreateJob");
        request["name"] = "AnotherJob";
        request["priority"] = "500";

        BedrockJobsCommand command(SQLiteCommand(move(request)), nullptr);
        command.populateCrashIdentifyingValues();

        ASSERT_EQUAL(command.crashIdentifyingValues.size(), 2u);
        ASSERT_TRUE(command.crashIdentifyingValues.count("name"));
        ASSERT_TRUE(command.crashIdentifyingValues.count("priority"));
    }
} __JobsCrashIdentifyingValuesTest;
