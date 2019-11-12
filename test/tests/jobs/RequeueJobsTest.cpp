#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct RequeueJobsTest : tpunit::TestFixture {
    RequeueJobsTest()
        : tpunit::TestFixture("RequeueJobs",
                              BEFORE_CLASS(RequeueJobsTest::setupClass),
                              TEST(RequeueJobsTest::requeueRunningJob),
                              TEST(RequeueJobsTest::requeueRunqueuedJob),
                              TEST(RequeueJobsTest::requeueMultipleJobs),
                              TEST(RequeueJobsTest::changeMultipleJobNames),
                              TEST(RequeueJobsTest::testNextRunTime),
                              AFTER(RequeueJobsTest::tearDown),
                              AFTER_CLASS(RequeueJobsTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(_threadID, {{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    void requeueRunningJob() {
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job, 
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Confirm the job is in RUNNING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Retry it
        command.clear();
        command.methodLine = "RequeueJobs";
        command["jobIDs"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confrim the job is back in the QUEUED state
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    void requeueRunqueuedJob() {
        SData command("CreateJob");
        command["name"] = "job";
        command["retryAfter"] = "+1 MINUTE";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job, 
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Confirm the job is in RUNQUEUED
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_EQUAL(result[0][0], "RUNQUEUED");

        // Retry it
        command.clear();
        command.methodLine = "RequeueJobs";
        command["jobIDs"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confrim the job is back in the QUEUED state
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    void requeueMultipleJobs() {
        SData command("CreateJob");
        command["name"] = "job1";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response["jobID"];

        command["name"] = "job2";
        command["retryAfter"] = "+1 MINUTE";
        response = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response["jobID"];

        command["name"] = "job3";
        response = tester->executeWaitVerifyContentTable(command);
        string jobID3 = response["jobID"];

        // Get the first job, 
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job1";
        tester->executeWaitVerifyContent(command);

        // Get the second job, 
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job2";
        tester->executeWaitVerifyContent(command);

        // Confirm the first job is RUNNING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1+ ";",  result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Confirm the first job is RUNQUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";",  result);
        ASSERT_EQUAL(result[0][0], "RUNQUEUED");

        // Confirm the third job is QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID3 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Requeue the jobs
        command.clear();
        command.methodLine = "RequeueJobs";
        command["jobIDs"] = jobID1 + ',' + jobID2 + "," + jobID3;
        tester->executeWaitVerifyContent(command);

        // Confirm all the jobs are QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID3 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    void changeMultipleJobNames() {
        SData command("CreateJob");
        command["name"] = "job1";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response["jobID"];

        command["name"] = "job2";
        command["retryAfter"] = "+1 MINUTE";
        response = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response["jobID"];

        command["name"] = "job3";
        response = tester->executeWaitVerifyContentTable(command);
        string jobID3 = response["jobID"];

        // Get the first job, 
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job1";
        tester->executeWaitVerifyContent(command);

        // Get the second job, 
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job2";
        tester->executeWaitVerifyContent(command);

        // Confirm the first job is RUNNING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1+ ";",  result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Confirm the first job is RUNQUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";",  result);
        ASSERT_EQUAL(result[0][0], "RUNQUEUED");

        // Confirm the third job is QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID3 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Requeue the jobs with a new name
        command.clear();
        command.methodLine = "RequeueJobs";
        command["jobIDs"] = jobID1 + ',' + jobID2 + "," + jobID3;
        command["name"] = "newJobName";
        tester->executeWaitVerifyContent(command);

        // Confirm all the jobs are QUEUED with a different name
        tester->readDB("SELECT state, name FROM jobs WHERE jobID = " + jobID1 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "newJobName");
        tester->readDB("SELECT state, name FROM jobs WHERE jobID = " + jobID2 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "newJobName");
        tester->readDB("SELECT state, name FROM jobs WHERE jobID = " + jobID3 + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "newJobName");
    }

    void testNextRunTime() {
        // Some time setup
        const uint64_t time = STimeNow();
        string oldTime = SComposeTime("%Y-%m-%d %H:%M:%S", time - 10'000'000);
        string currentTime = SComposeTime("%Y-%m-%d %H:%M:%S", time - 10'000'000);

        // Create the job we will requeue
        SData command("CreateJob");
        command["name"] = "job";
        command["nextRun"] = currentTime;
        command["created"] = oldTime;
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Requeue the job
        command.clear();
        command.methodLine = "RequeueJobs";
        command["jobIDs"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Verify that nextRun = created
        SQResult result;
        tester->readDB("SELECT created, nextRun FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_EQUAL(result[0][0], result[0][1]);
    }

} __RequeueJobsTest;
