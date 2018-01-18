#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct CreateJobTest : tpunit::TestFixture {
    CreateJobTest()
        : tpunit::TestFixture("CreateJob",
                              BEFORE_CLASS(CreateJobTest::setupClass),
                              TEST(CreateJobTest::create),
                              TEST(CreateJobTest::createWithHttp),
                              TEST(CreateJobTest::createWithPriority),
                              TEST(CreateJobTest::createWithData),
                              TEST(CreateJobTest::createWithRepeat),
                              TEST(CreateJobTest::uniqueJob),
                              TEST(CreateJobTest::createWithBadData),
                              TEST(CreateJobTest::createWithBadRepeat),
                              TEST(CreateJobTest::createChildWithQueuedParent),
                              TEST(CreateJobTest::createChildWithRunningGrandparent),
                              TEST(CreateJobTest::retryRecurringJobs),
                              TEST(CreateJobTest::retryWithMalformedValue),
                              TEST(CreateJobTest::retryUnique),
                              TEST(CreateJobTest::retryLifecycle),
                              TEST(CreateJobTest::retryWithChildren),
                              AFTER(CreateJobTest::tearDown),
                              AFTER_CLASS(CreateJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    void create() {
        SData command("CreateJob");
        string jobName = "testCreate";
        command["name"] = jobName;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    void createWithHttp() {
        SData command("CreateJob / HTTP/1.1");
        string jobName = "testCreate";
        command["name"] = jobName;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    void createWithPriority() {
        SData command("CreateJob");
        string jobName = "testCreate";
        string priority = "1000";
        command["name"] = jobName;
        command["priority"] = priority;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(originalJob[0][8], priority);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    void createWithData() {
        SData command("CreateJob");
        string jobName = "testCreate";
        string data = "{\"blabla\":\"blabla\"}";
        command["name"] = jobName;
        command["data"] = data;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], data);
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    void createWithRepeat() {
        SData command("CreateJob");
        string jobName = "testCreate";
        string repeat = "SCHEDULED, +1 HOUR";
        command["name"] = jobName;
        command["repeat"] = repeat;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], repeat);
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    // Create a unique job
    // Then try to recreate the job with the some data
    // Make sure the new data is saved
    void uniqueJob() {
        // Create a unique job
        SData command("CreateJob");
        string jobName = "blabla";
        command["name"] = jobName;
        command["unique"] = "true";
        STable response = tester->executeWaitVerifyContentTable(command);
        int jobID = SToInt(response["jobID"]);
        ASSERT_GREATER_THAN(jobID, 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);

        // Try to recreate the job with the same data.
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(SToInt(response["jobID"]), jobID);

        // Try to recreate the job with new data, it should get updated.
        string data = "{\"blabla\":\"test\"}";
        command["data"] = data;
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(SToInt(response["jobID"]), jobID);

        SQResult updatedJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", updatedJob);
        ASSERT_EQUAL(updatedJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(updatedJob[0][0], originalJob[0][0]);
        ASSERT_EQUAL(updatedJob[0][1], originalJob[0][1]);
        ASSERT_EQUAL(updatedJob[0][2], originalJob[0][2]);
        ASSERT_EQUAL(updatedJob[0][3], originalJob[0][3]);
        ASSERT_EQUAL(updatedJob[0][4], originalJob[0][4]);
        ASSERT_EQUAL(updatedJob[0][5], originalJob[0][5]);
        ASSERT_EQUAL(updatedJob[0][6], originalJob[0][6]);
        ASSERT_EQUAL(updatedJob[0][7], data);
        ASSERT_EQUAL(updatedJob[0][8], originalJob[0][8]);
        ASSERT_EQUAL(updatedJob[0][9], originalJob[0][9]);
    }

    void createWithBadData() {
        SData command("CreateJob");
        command["name"] = "blabla";
        command["data"] = "blabla";
        tester->executeWaitVerifyContent(command, "402 Data is not a valid JSON Object");
    }

    void createWithBadRepeat() {
        SData command("CreateJob");
        command["name"] = "blabla";
        command["repeat"] = "blabla";
        tester->executeWaitVerifyContent(command, "402 Malformed repeat");
    }

    // Cannot create a child job when parent is QUEUED
    void createChildWithQueuedParent() {
        // Create a parent job
        SData command("CreateJob");
        command["name"] = "parent";

        STable response = tester->executeWaitVerifyContentTable(command);
        string parentID = response["jobID"];

        // Try to create the child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child";
        command["parentJobID"] = parentID;
        tester->executeWaitVerifyContent(command, "405 Can only create child job when parent is RUNNING or PAUSED");
    }

    // Cannot create a job with a running grandparent
    void createChildWithRunningGrandparent() {
        // Create a parent job
        SData command("CreateJob");
        command["name"] = "parent";
        STable response = tester->executeWaitVerifyContentTable(command);
        string parentID = response["jobID"];

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWaitVerifyContent(command);

        // Create the child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string childID = response["jobID"];

        // Assert parent is still running
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Try to create grandchild
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "grandchild";
        command["parentJobID"] = childID;
        tester->executeWaitVerifyContent(command, "405 Cannot create grandchildren");
    }

    void retryRecurringJobs() {
        SData command("CreateJob");
        command["name"] = "test";
        command["repeat"] = "SCHEDULED, +1 HOUR";
        command["retryAfter"] = "10";
        tester->executeWaitVerifyContent(command, "402 Recurring auto-retrying jobs are not supported");
    }

    void retryWithMalformedValue() {
        SData command("CreateJob");
        command["name"] = "test";
        command["retryAfter"] = "10";
        tester->executeWaitVerifyContent(command, "402 Malformed retryAfter");
    }

    void retryUnique() {
        SData command("CreateJob");
        command["name"] = "test";
        command["retryAfter"] = "+10 HOUR";
        command["unique"] = "true";
        tester->executeWaitVerifyContent(command, "405 Unique jobs can't be retried");
    }

    void retryLifecycle() {
        // Create a retryable job
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+1 SECOND";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;

        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Query the db to confirm it was created correctly
        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID, retryAfter FROM jobs WHERE jobID = " + jobID + ";", originalJob);
        ASSERT_EQUAL(originalJob[0][1], jobID);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
        ASSERT_EQUAL(originalJob[0][10], retryValue);

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = jobName;
        response = tester->executeWaitVerifyContentTable(command);

        ASSERT_EQUAL(response["data"], "{}");
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);

        // Query the db and confirm that state, nextRun and lastRun are 1 second apart
        SQResult jobData;
        tester->readDB("SELECT state, nextRun, lastRun FROM jobs WHERE jobID = " + jobID + ";", jobData);
        ASSERT_EQUAL(jobData[0][0], "RUNQUEUED");
        time_t nextRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][1]);
        time_t lastRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][2]);
        ASSERT_EQUAL(difftime(nextRunTime, lastRunTime), 1);

        // Get the job, confirm error
        tester->executeWaitVerifyContent(command, "404 No job found");

        // Wait 1 second, get the job, confirm no error
        sleep(1);
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["data"], "{}");
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);

        // Get the job, confirm error
        tester->executeWaitVerifyContent(command, "404 No job found");

        // Finish the job
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Query db and confirm job doesn't exist
        tester->readDB("SELECT state, nextRun, lastRun, FROM jobs WHERE jobID = " + jobID + ";", jobData);
        ASSERT_TRUE(jobData.empty());
    }

    void retryWithChildren() {
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+5 SECONDS";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;

        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Try to create child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testRetryableChild";
        command["parentJobID"] = jobID;
        tester->executeWaitVerifyContent(command, "402 Auto-retrying parents cannot have children");
    }
} __CreateJobTest;
