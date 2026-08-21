#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct FailJobTest : tpunit::TestFixture
{
    FailJobTest()
        : tpunit::TestFixture("FailJob",
                              BEFORE_CLASS(FailJobTest::setupClass),
                              TEST(FailJobTest::nonExistentJob),
                              TEST(FailJobTest::notInRunningRunqueuedState),
                              TEST(FailJobTest::failJobInRunningState),
                              TEST(FailJobTest::failJobInRunqueuedState),
                              TEST(FailJobTest::uniqueAsRetry),
                              AFTER(FailJobTest::tearDown),
                              AFTER_CLASS(FailJobTest::tearDownClass))
    {
    }

    BedrockTester* tester;

    void setupClass()
    {
        tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});
    }

    // Reset the jobs table
    void tearDown()
    {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass()
    {
        delete tester;
    }

    // Throw an error if the job doesn't exist
    void nonExistentJob()
    {
        SData command("FailJob");
        command["jobID"] = "1";
        tester->executeWaitVerifyContent(command, "404 No job with this jobID");
    }

    // Throw an error if the job is not in RUNNING or REQUEUED state
    void notInRunningRunqueuedState()
    {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Fail it
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can only fail RUNNING or RUNQUEUED jobs");
    }

    // Fail job in RUNNING state
    void failJobInRunningState()
    {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Assert job is in RUNNING state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Fail it
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Failing the job should succeed and set it as FAILED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");
    }

    // Fail job in RUNQUEUED state
    void failJobInRunqueuedState()
    {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        command["retryAfter"] = "+1 MINUTES";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Confirm the job is in RUNQUEUED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNQUEUED");

        // Fail it
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Failing the job should succeed and set it as FAILED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");
    }

    void uniqueAsRetry()
    {
        // Given an opted-in unique job is ready for its first worker
        SData command("CreateJob");
        command["name"] = "failComparedData";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string jobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "failComparedData";
        STable runningJob = tester->executeWaitVerifyContentTable(command);
        const string expectedData = runningJob["data"];

        // expectedData must be a JSON object.
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["expectedData"] = "not-json";
        tester->executeWaitVerifyContent(command, "402 expectedData is not a valid JSON Object");

        // A newer enqueue changes the current payload while the worker is running.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "failComparedData";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        // The old worker cannot fail the new work or overwrite its payload.
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["expectedData"] = expectedData;
        command["data"] = "{\"activity\":1,\"worker\":true}";
        tester->executeWaitVerifyContent(command);

        SQResult result;
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockUniqueAsRetry'), "
                       "JSON_EXTRACT(data, '$.activity'), JSON_EXTRACT(data, '$.worker') "
                       "FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "1");
        ASSERT_EQUAL(result[0][2], "2");
        ASSERT_EQUAL(result[0][3], "");

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "failComparedData";
        runningJob = tester->executeWaitVerifyContentTable(command);

        // The subsequent worker can fail normally when its snapshot still matches.
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["expectedData"] = runningJob["data"];
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT state FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");

        // Missing expectedData preserves legacy behavior during a rolling BWM deployment.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "legacyFailure";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string legacyJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "legacyFailure";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "legacyFailure";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = legacyJobID;
        tester->executeWaitVerifyContent(command);
        tester->readDB("SELECT state FROM jobs WHERE jobID=" + legacyJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");
    }
} __FailJobTest;
