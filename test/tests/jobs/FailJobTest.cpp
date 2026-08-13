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
        command["name"] = "failVersioned";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        command["requestID"] = "fail-1";
        const string jobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        // When a worker dequeues the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "failVersioned";
        STable runningJob = tester->executeWaitVerifyContentTable(command);

        // Then the worker receives version 1 because it owns the first run
        ASSERT_EQUAL(runningJob["enqueueVersion"], "1");

        // Given version 1 is active
        // When FailJob omits the active version
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;

        // Then Bedrock rejects the request because an unversioned failure can belong to an older run
        tester->executeWaitVerifyContent(command, "402 Missing enqueueVersion");

        // Given version 1 remains active and a distinct enqueue creates version 2
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "failVersioned";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        command["requestID"] = "fail-2";
        tester->executeWaitVerifyContent(command);

        // When the worker for version 1 reports a fatal result with stale data
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["enqueueVersion"] = "1";
        command["data"] = "{\"activity\":1,\"worker\":true}";
        tester->executeWaitVerifyContent(command);

        // Then Bedrock queues version 2 because its work must survive the failure from the older worker
        SQResult result;
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockUniqueAsRetry.version'), "
                       "JSON_EXTRACT(data, '$.activity'), JSON_EXTRACT(data, '$.worker') "
                       "FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "2");
        ASSERT_EQUAL(result[0][2], "2");
        ASSERT_EQUAL(result[0][3], "");

        // Given version 2 is queued after the stale failure
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "failVersioned";

        // When the next worker dequeues the job
        runningJob = tester->executeWaitVerifyContentTable(command);

        // Then the worker receives version 2 because that version owns the current run
        ASSERT_EQUAL(runningJob["enqueueVersion"], "2");

        // Given version 2 is active
        // When a delayed failure from version 1 arrives
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["enqueueVersion"] = "1";
        tester->executeWaitVerifyContent(command);

        // Then Bedrock keeps version 2 active because version 1 no longer owns the job
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$.activity') FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
        ASSERT_EQUAL(result[0][1], "2");

        // Given version 2 remains active after the stale failure
        command["enqueueVersion"] = "2";

        // When its worker reports the fatal result with the current version
        tester->executeWaitVerifyContent(command);

        // Then Bedrock marks the job as failed because no newer enqueue needs to run
        tester->readDB("SELECT state FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");
    }
} __FailJobTest;
