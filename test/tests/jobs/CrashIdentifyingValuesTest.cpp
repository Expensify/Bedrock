#include <libstuff/SData.h>
#include <plugins/Jobs.h>
#include <test/lib/BedrockTester.h>

// Unit tests for BedrockJobsCommand::populateCrashIdentifyingValues(). The crash blacklist
// ("poison-pill" protection) refuses any future command whose methodLine and crashIdentifyingValues
// exactly match a command that previously crashed a node. crashIdentifyingValues is keyed on an
// explicit whitelist of the fields that semantically identify a Jobs command -- `name` and `data`.
// Everything else (e.g. `requestID`, `jobID`, `priority`, `lastIP`) is volatile or non-identifying
// per-request data. Keying only on `name`/`data` keeps the same command's repeated crashes on a
// single blacklist entry, and lets BedrockServer::_wouldCrash's "more than one identifying set ->
// block everyone" safeguard fire only when genuinely different commands crash. These tests verify
// only the whitelisted fields are kept and everything else is dropped.
struct JobsCrashIdentifyingValuesTest : tpunit::TestFixture
{
    JobsCrashIdentifyingValuesTest()
        : tpunit::TestFixture("JobsCrashIdentifyingValues",
                              TEST(JobsCrashIdentifyingValuesTest::onlyKeepsWhitelistedFields),
                              TEST(JobsCrashIdentifyingValuesTest::keepsOnlyPresentWhitelistedFields))
    {
    }

    // Only the whitelisted fields (`name`, `data`) and their values are kept; every other field --
    // including per-request volatile fields -- is dropped.
    void onlyKeepsWhitelistedFields()
    {
        SData request("GetJob");
        request["name"] = "TestJob";
        request["data"] = "{\"key\":\"value\"}";
        request["jobID"] = "12345";
        request["priority"] = "500";
        request["requestID"] = "abc123";
        request["ID"] = "999";
        request["lastIP"] = "127.0.0.1";
        request["_source"] = "someSource";

        BedrockJobsCommand command(SQLiteCommand(move(request)), nullptr);
        command.populateCrashIdentifyingValues();

        // Only the whitelisted fields are kept, with their original values.
        ASSERT_EQUAL(command.crashIdentifyingValues.size(), 2u);
        ASSERT_TRUE(command.crashIdentifyingValues.count("name"));
        ASSERT_TRUE(command.crashIdentifyingValues.count("data"));
        ASSERT_EQUAL(command.crashIdentifyingValues["name"], "TestJob");
        ASSERT_EQUAL(command.crashIdentifyingValues["data"], "{\"key\":\"value\"}");

        // Every non-whitelisted field must be excluded -- including non-volatile-but-non-identifying
        // fields like jobID/priority, so the same command crashing twice stays on one blacklist entry.
        ASSERT_FALSE(command.crashIdentifyingValues.count("jobID"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("priority"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("requestID"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("ID"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("lastIP"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("_source"));
    }

    // A whitelisted field that isn't set on the request is not recorded (CrashMap::insert skips
    // fields the request doesn't have).
    void keepsOnlyPresentWhitelistedFields()
    {
        SData request("CreateJob");
        request["name"] = "AnotherJob";
        request["priority"] = "500";

        BedrockJobsCommand command(SQLiteCommand(move(request)), nullptr);
        command.populateCrashIdentifyingValues();

        // `data` was never set, so only `name` should be present; `priority` is not whitelisted.
        ASSERT_EQUAL(command.crashIdentifyingValues.size(), 1u);
        ASSERT_TRUE(command.crashIdentifyingValues.count("name"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("data"));
        ASSERT_FALSE(command.crashIdentifyingValues.count("priority"));
    }
} __JobsCrashIdentifyingValuesTest;
