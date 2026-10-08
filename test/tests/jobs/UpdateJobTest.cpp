#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct UpdateJobTest : tpunit::TestFixture
{
    UpdateJobTest()
        : tpunit::TestFixture("UpdateJob",
                              BEFORE_CLASS(UpdateJobTest::setupClass),
                              TEST(UpdateJobTest::updateJob),
                              TEST(UpdateJobTest::updateStringValueLookingLikeNumber),
                              TEST(UpdateJobTest::updateMockedJob),
                              TEST(UpdateJobTest::dataUpdatePreservesOriginalNextRun),
                              TEST(UpdateJobTest::scheduleUpdateDoesNotRestoreOriginalNextRun),
                              TEST(UpdateJobTest::clearRepeatWithShouldClearRepeat),
                              AFTER_CLASS(UpdateJobTest::tearDownClass))
    {
    }

    BedrockTester* tester;

    void setupClass()
    {
        tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});
    }

    void tearDownClass()
    {
        delete tester;
    }

    // Simple UpdateJob with all parameters
    void updateJob()
    {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        string oldPriority = "500";
        command["jobPriority"] = oldPriority;
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];
        ASSERT_GREATER_THAN(stol(jobID), 0);

        // Call the UpdateJob command
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{\"key\":\"value\"}";
        command["repeat"] = "HOURLY";
        command["jobPriority"] = "1000";
        command["nextRun"] = "2020-01-01 00:00:00";
        tester->executeWaitVerifyContent(command);

        // Verify that the job was actually updated
        SQResult currentJob;
        tester->readDB("SELECT repeat, data, priority, nextRun FROM jobs WHERE jobID = " + jobID + ";", currentJob);
        ASSERT_EQUAL(currentJob[0][0], "HOURLY");
        ASSERT_EQUAL(currentJob[0][1], "{\"key\":\"value\"}");
        ASSERT_EQUAL(currentJob[0][2], "1000");
        ASSERT_NOT_EQUAL(currentJob[0][2], oldPriority);
        ASSERT_EQUAL(currentJob[0][3], "2020-01-01 00:00:00");
    }

    void updateStringValueLookingLikeNumber()
    {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["data"] = "{\"key\":\"value\",\"anotherKey\":\"123\"}";
        string oldPriority = "500";
        command["jobPriority"] = oldPriority;
        STable response = tester->executeWaitVerifyContentTable(command);
        const string jobID = response["jobID"];
        ASSERT_GREATER_THAN(stol(jobID), 0);

        SQResult currentJob;
        tester->readDB("SELECT data FROM jobs WHERE jobID = " + jobID + ";", currentJob);
        ASSERT_EQUAL("{\"key\":\"value\",\"anotherKey\":\"123\"}", currentJob[0][0]);

        // Call the UpdateJob command
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{\"key\":\"value\",\"anotherKey\":\"1234\"}";
        command["repeat"] = "HOURLY";
        command["jobPriority"] = "1000";
        command["nextRun"] = "2020-01-01 00:00:00";
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT data FROM jobs WHERE jobID = " + jobID + ";", currentJob);
        ASSERT_EQUAL("{\"key\":\"value\",\"anotherKey\":\"1234\"}", currentJob[0][0]);
    }

    void updateMockedJob()
    {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        string oldPriority = "500";
        command["jobPriority"] = oldPriority;
        command["mockRequest"] = "1";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];
        ASSERT_GREATER_THAN(stol(jobID), 0);

        // Call the UpdateJob command
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{\"key\":\"value\"}";
        command["repeat"] = "HOURLY";
        command["jobPriority"] = "1000";
        command["nextRun"] = "2020-01-01 00:00:00";
        tester->executeWaitVerifyContent(command);

        // Verify that the job was actually updated
        SQResult currentJob;
        tester->readDB("SELECT repeat, data, priority, nextRun FROM jobs WHERE jobID = " + jobID + ";", currentJob);
        ASSERT_EQUAL(currentJob[0][0], "HOURLY");
        ASSERT_EQUAL(currentJob[0][1], "{\"key\":\"value\",\"mockRequest\":true}");
        ASSERT_EQUAL(currentJob[0][2], "1000");
        ASSERT_NOT_EQUAL(currentJob[0][2], oldPriority);
        ASSERT_EQUAL(currentJob[0][3], "2020-01-01 00:00:00");
    }

    void dataUpdatePreservesOriginalNextRun()
    {
        const string firstRun = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow() - STIME_US_PER_S);

        SData command("CreateJob");
        command["name"] = "data-update";
        command["firstRun"] = firstRun;
        command["repeat"] = "SCHEDULED, +1 DAY";
        command["retryAfter"] = "+5 MINUTES";
        command["data"] = "{\"phase\":\"initial\",\"obsolete\":true}";
        const string jobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "data-update";
        STable runningJob = tester->executeWaitVerifyContentTable(command);
        ASSERT_TRUE(SParseJSONObject(runningJob["data"])["originalNextRun"].empty());

        SQResult before;
        tester->readDB("SELECT nextRun, JSON_EXTRACT(data, '$.originalNextRun') FROM jobs WHERE jobID=" + jobID + ";", before);
        ASSERT_EQUAL(before[0][1], firstRun);

        // The worker's replacement data lacks the anchor GetJob stored after taking its data snapshot.
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{\"phase\":\"updated\"}";
        tester->executeWaitVerifyContent(command);

        SQResult result;
        tester->readDB("SELECT state, nextRun, data FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNQUEUED");
        ASSERT_EQUAL(result[0][1], before[0][0]);
        STable updatedData = SParseJSONObject(result[0][2]);
        ASSERT_EQUAL(updatedData["originalNextRun"], firstRun);
        ASSERT_EQUAL(updatedData["phase"], "updated");
        ASSERT_FALSE(SContains(updatedData, "obsolete"));

        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT state, nextRun, JSON_EXTRACT(data, '$.originalNextRun') FROM jobs WHERE jobID=" + jobID + ";", result);
        SQResult expected;
        tester->readDB("SELECT DATETIME(" + SQ(firstRun) + ", '+1 DAY');", expected);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], expected[0][0]);
        ASSERT_TRUE(result[0][2].empty());
    }

    void scheduleUpdateDoesNotRestoreOriginalNextRun()
    {
        SData command("CreateJob");
        command["name"] = "schedule-update";
        command["repeat"] = "SCHEDULED, +1 DAY";
        command["retryAfter"] = "+5 MINUTES";
        const string jobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "schedule-update";
        tester->executeWaitVerifyContent(command);

        SQResult result;
        tester->readDB("SELECT JSON_EXTRACT(data, '$.originalNextRun') FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_FALSE(result[0][0].empty());

        const string nextRun = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow() + STIME_US_PER_S * 60 * 60 * 24);
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{\"phase\":\"rescheduled\"}";
        command["nextRun"] = nextRun;
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT nextRun, data FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], nextRun);
        ASSERT_EQUAL(result[0][1], "{\"phase\":\"rescheduled\"}");
    }

    void clearRepeatWithShouldClearRepeat()
    {
        // Create a repeating job
        SData command("CreateJob");
        command["name"] = "repeating-job";
        command["repeat"] = "HOURLY";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];
        ASSERT_GREATER_THAN(stol(jobID), 0);

        // Confirm repeat is set
        SQResult before;
        tester->readDB("SELECT repeat FROM jobs WHERE jobID = " + jobID + ";", before);
        ASSERT_EQUAL(before[0][0], "HOURLY");

        // UpdateJob with shouldClearRepeat=1 — should set repeat to "" in the DB
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{}";
        command["shouldClearRepeat"] = "1";
        tester->executeWaitVerifyContent(command);

        // Verify repeat is now cleared
        SQResult after;
        tester->readDB("SELECT repeat FROM jobs WHERE jobID = " + jobID + ";", after);
        ASSERT_EQUAL(after[0][0], "");
    }
} __UpdateJobTest;
