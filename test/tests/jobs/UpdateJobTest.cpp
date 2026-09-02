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
                              TEST(UpdateJobTest::uniqueAsRetryRequiresStrictObjectData),
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

    void uniqueAsRetryRequiresStrictObjectData()
    {
        SData command("CreateJob");
        command["name"] = "strict-update";
        command["data"] = "{\"value\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string jobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        for (const string& invalidData : {string("[]"), string("{\"value\":1,\"value\":2}")}) {
            command.clear();
            command.methodLine = "UpdateJob";
            command["jobID"] = jobID;
            command["data"] = invalidData;
            tester->executeWaitVerifyContent(command, "402 Data is not a valid JSON Object");
        }

        SQResult result;
        tester->readDB("SELECT JSON_TYPE(data, '$._bedrockRerunIfDataChanged'), JSON_EXTRACT(data, '$.value') "
                       "FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "true");
        ASSERT_EQUAL(result[0][1], "1");

        command.clear();
        command.methodLine = "Query";
        command["query"] = "UPDATE jobs SET data = "
            "'{\"_bedrockRerunIfDataChanged\":true,\"value\":1,\"value\":2}' WHERE jobID = " + jobID + ";";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{\"value\":3}";
        tester->executeWaitVerifyContent(command, "500 Opted-in job contains invalid JSON data");
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
