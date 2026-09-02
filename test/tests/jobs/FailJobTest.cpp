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
                              TEST(FailJobTest::rerunIfDataChanged),
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

    void rerunIfDataChanged()
    {
        // Given an opted-in unique job that is ready for its first worker
        SData command("CreateJob");
        command["name"] = "failComparedData";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["rerunIfDataChanged"] = "true";
        const string jobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "failComparedData";
        STable runningJob = tester->executeWaitVerifyContentTable(command);
        const string expectedData = runningJob["data"];

        // When the worker submits malformed expected data that Bedrock cannot compare semantically
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["expectedData"] = "not-json";

        // Then Bedrock rejects the failure because it cannot make an atomic freshness decision
        tester->executeWaitVerifyContent(command, "402 expectedData is not a valid JSON Object");

        // Given a duplicate enqueue that installs newer activity while the worker remains active
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "failComparedData";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["rerunIfDataChanged"] = "true";
        tester->executeWaitVerifyContent(command);

        // When the stale worker reports a fatal result with output from the original activity
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["expectedData"] = expectedData;
        command["data"] = "{\"activity\":1,\"worker\":true}";
        tester->executeWaitVerifyContent(command);

        // Then Bedrock queues the newer activity because the stale result does not own it
        SQResult result;
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockRerunIfDataChanged'), "
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

        // Given the subsequent worker owns the current activity
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID;
        command["expectedData"] = runningJob["data"];

        // When that worker reports a fatal result
        tester->executeWaitVerifyContent(command);

        // Then Bedrock fails the row because no newer activity needs another run
        tester->readDB("SELECT state FROM jobs WHERE jobID=" + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");

        // Given an opted-in worker that reports progress without a duplicate enqueue
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "progressOnlyFailure";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["rerunIfDataChanged"] = "true";
        const string progressOnlyJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "progressOnlyFailure";
        runningJob = tester->executeWaitVerifyContentTable(command);

        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = progressOnlyJobID;
        command["data"] = "{\"activity\":1,\"progress\":50}";
        tester->executeWaitVerifyContent(command);

        // When the worker reports a fatal result with its immutable dequeue snapshot
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = progressOnlyJobID;
        command["expectedData"] = runningJob["data"];
        tester->executeWaitVerifyContent(command);

        // Then Bedrock requeues the job because its current data no longer matches the immutable snapshot
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$.progress') FROM jobs WHERE jobID=" +
                       progressOnlyJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "50");

        // Given an opted-in job that receives newer activity from a duplicate enqueue
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "legacyFailure";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["rerunIfDataChanged"] = "true";
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
        command["rerunIfDataChanged"] = "true";
        tester->executeWaitVerifyContent(command);

        // When an old worker manager reports failure without expectedData
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = legacyJobID;
        tester->executeWaitVerifyContent(command);

        // Then Bedrock uses legacy failure behavior because rolling deployments require backward compatibility
        tester->readDB("SELECT state FROM jobs WHERE jobID=" + legacyJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");
    }
} __FailJobTest;
