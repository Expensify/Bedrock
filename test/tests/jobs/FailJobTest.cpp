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
                              TEST(FailJobTest::failJobPromotesWaitingJob),
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

    // FailJob should promote the next WAITING job with same sequentialKey
    void failJobPromotesWaitingJob()
    {
        // Create first job
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_fail";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        // Create second job
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_fail";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        // Verify second job is WAITING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");

        // Get first job to put it in RUNNING state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "testSequential1";
        tester->executeWaitVerifyContent(command);

        // Fail first job
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID1;
        tester->executeWaitVerifyContent(command);

        // Verify first job is FAILED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");

        // Verify second job is now QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }
} __FailJobTest;
