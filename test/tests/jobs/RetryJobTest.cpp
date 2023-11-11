#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct RetryJobTest : tpunit::TestFixture {
    RetryJobTest()
        : tpunit::TestFixture("RetryJob",
                              BEFORE_CLASS(RetryJobTest::setupClass),
                              TEST(RetryJobTest::nonExistentJob),
                              TEST(RetryJobTest::notInRunningState),
                              TEST(RetryJobTest::parentIsNotPaused),
                              TEST(RetryJobTest::removeFinishedAndCancelledChildren),
                              TEST(RetryJobTest::updateData),
                              TEST(RetryJobTest::negativeDelay),
                              TEST(RetryJobTest::positiveDelay),
                              TEST(RetryJobTest::delayError),
                              TEST(RetryJobTest::hasRepeat),
                              TEST(RetryJobTest::hasRepeatStartOfHour),
                              TEST(RetryJobTest::inRunqueuedState),
                              TEST(RetryJobTest::simplyRetryWithNextRun),
                              TEST(RetryJobTest::changeNameAndPriority),
                              TEST(RetryJobTest::changeName),
                              TEST(RetryJobTest::changePriority),
                              TEST(RetryJobTest::hasRepeatWithNextRun),
                              TEST(RetryJobTest::hasRepeatWithDelay),
                              TEST(RetryJobTest::hasRepeatWithNextRunIgnoreRepeat),
                              TEST(RetryJobTest::hasDelayAndNextRun),
                              TEST(RetryJobTest::simpleRetryWithHttp),
                              AFTER(RetryJobTest::tearDown),
                              AFTER_CLASS(RetryJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    // Throw an error if the job doesn't exist
    void nonExistentJob() {
        SData command("RetryJob");
        command["jobID"] = "1";
        tester->executeWaitVerifyContent(command, "404 No job with this jobID");
    }

    // Throw an error if the job is not in RUNNING state
    void notInRunningState() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can only retry/finish RUNNING and RUNQUEUED jobs");
    }

    // If job has a parentID, the parent should be paused
    void parentIsNotPaused() {
        // Create the parent
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

        // It's not possible to put the child in the QUEUED state without the parent being paused
        // and a child cannot being the RUNNING state without first being the QUEUED state
        // but we check for this to make sure something funky didn't occur.
        // We'll manually put the child in the RUNNING state to hit this condition
        command.clear();
        command.methodLine = "Query";
        command["query"] = "UPDATE jobs SET state = 'RUNNING' WHERE jobID = " + childID + ";";
        tester->executeWaitVerifyContent(command);

        // Retry the child
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command, "405 Can only retry/finish child job when parent is PAUSED");
    }

    // Child jobs that are in the FINISHED or CANCELLED state should be deleted when the parent is finished
    void removeFinishedAndCancelledChildren() {
        // Create the parent
        SData command("CreateJob");
        command["name"] = "parent";
        STable response = tester->executeWaitVerifyContentTable(command);
        string parentID = response["jobID"];

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWaitVerifyContent(command);

        // Create the children
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_finished";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string finishedChildID = response["jobID"];
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_cancelled";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string cancelledChildID = response["jobID"];
        command.clear();

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get a child job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child_finished";
        tester->executeWaitVerifyContent(command);

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

        // Cancel a child
        // if this goes 2nd this doesn't retry the parent job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = cancelledChildID;
        tester->executeWaitVerifyContent(command);

        // Finish a child
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishedChildID;
        tester->executeWaitVerifyContent(command);

        // Retry the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Confirm that the FINISHED and CANCELLED children are deleted
        SQResult result;
        tester->readDB("SELECT count(*) FROM jobs WHERE jobID != " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL;", result);
        ASSERT_EQUAL(SToInt(result[0][0]), 0);
    }

    // Update the job data if new data is passed
    void updateData() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        STable data;
        data["foo"] = "bar";
        data["bar"] = "foo";
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["data"] = SComposeJSONObject(data);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT data FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], SComposeJSONObject(data));
    }

    // Cannot retry with a negative delay
    void negativeDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["delay"] = "-5";
        tester->executeWaitVerifyContent(command, "402 Must specify a non-negative delay when retrying");
    }

    // Retry with a positive delay and confirm nextRun is updated appropriately
    void positiveDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get a timestamp from before we update nextRun.
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        string minimumLastRun = result[0][0];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWaitVerifyContent(command);

        // Get a timestamp from after we update nextRun.
        tester->readDB("SELECT DATETIME();", result);
        string maximumLastRun = result[0][0];

        // Get the actual value of nextRun.
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        string actualNextRun = result[0][0];

        // Because nextRun is updated during RetryJob based on the *current time*, we don't know exactly what it should
        // be, but we do have two values that can't be more/less than this, so make sure it falls in this range.
        time_t minimumLastRunTime = JobTestHelper::getTimestampForDateTimeString(minimumLastRun);
        time_t maximumLastRunTime = JobTestHelper::getTimestampForDateTimeString(maximumLastRun);
        time_t actualNextRunTime = JobTestHelper::getTimestampForDateTimeString(actualNextRun);

        double timeFromStart = difftime(actualNextRunTime, minimumLastRunTime);
        double timeFromEnd = difftime(actualNextRunTime, maximumLastRunTime);

        // Time from end must be <= 5, time from start must be >= 5.
        ASSERT_TRUE(timeFromStart >= 5);
        ASSERT_TRUE(timeFromEnd <= 5);
    }

    // Retry with too large of a delay
    void delayError() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the nextRun value
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        string originalNextRun = result[0][0];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["delay"] = "100000000";
        tester->executeWaitVerifyContent(command, "402 Malformed delay");
    }

    // Retry a job with a repeat
    void hasRepeat() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is in 1 hour
        SQResult result;
        tester->readDB("SELECT lastRun, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        time_t createdTime = JobTestHelper::getTimestampForDateTimeString(result[0][0]);
        time_t nextRunTime = JobTestHelper::getTimestampForDateTimeString(result[0][1]);
        ASSERT_EQUAL(difftime(nextRunTime, createdTime), 3600);
    }

    // Retry a job with a repeat that uses the custom "START OF HOUR" modifier
    void hasRepeatStartOfHour() {
        uint64_t now = STimeNow();

        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, START OF DAY, +5 MINUTES, START OF HOUR";
        command["nextRun"] = now;
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is at the beginning of the day and not 5 minutes after
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_EQUAL(result[0][0], SComposeTime("%Y-%m-%d 00:00:00", now));
    }

    // Retry job in RUNQUEUED state
    void inRunqueuedState() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        command["retryAfter"] = "+1 SECOND";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
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
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confrim the job is back in the QUEUED state
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    // Confirm nextRun is updated appropriately
    void simplyRetryWithNextRun() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        const string nextRun = getTimeInFuture(10);
        command["nextRun"] = nextRun;
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun updated correctly
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], nextRun);
    }

    // Update the name and priority
    void changeNameAndPriority() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["jobPriority"] = "500";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it passing name and priority
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["name"] = "newName";
        command["jobPriority"] = "1000";
        command["nextRun"] = getTimeInFuture(10);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT name, priority FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "newName");
        ASSERT_EQUAL(result[0][1], "1000");
    }

    void changeName() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["jobPriority"] = "500";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it passing only name
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["name"] = "newName";
        command["nextRun"] = getTimeInFuture(10);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT name, priority FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "newName");
        ASSERT_EQUAL(result[0][1], "500");
    }

    void changePriority() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["jobPriority"] = "500";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it passing only priority
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["jobPriority"] = "1000";
        command["nextRun"] = getTimeInFuture(10);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT name, priority FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "job");
        ASSERT_EQUAL(result[0][1], "1000");
    }

    // Repeat should take precedence over nextRun
    void hasRepeatWithNextRun() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["nextRun"] = "2017-09-07 23:11:11";
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is in 1 hour, not in the given nextRun time
        SQResult result;
        tester->readDB("SELECT lastRun, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_FLOAT_EQUAL(difftime(JobTestHelper::getTimestampForDateTimeString(result[0][1]), JobTestHelper::getTimestampForDateTimeString(result[0][0])), 3600);
    }

    // Repeat should take precedence over delay
    void hasRepeatWithDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is in 1 hour, not in the 5 second delay
        SQResult result;
        tester->readDB("SELECT lastRun, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_FLOAT_EQUAL(difftime(JobTestHelper::getTimestampForDateTimeString(result[0][1]), JobTestHelper::getTimestampForDateTimeString(result[0][0])), 3600);
    }

    void hasRepeatWithNextRunIgnoreRepeat() {
         // Create the job
         SData command("CreateJob");
         command["name"] = "job";
         command["repeat"] = "STARTED, +1 HOUR";
         STable response = tester->executeWaitVerifyContentTable(command);
         string jobID = response["jobID"];

         // Get the job
         command.clear();
         command.methodLine = "GetJob";
         command["name"] = "job";
         tester->executeWaitVerifyContent(command);

         // Retry it
         command.clear();
         command.methodLine = "RetryJob";
         command["jobID"] = jobID;
         string nextRun = getTimeInFuture(10);
         command["nextRun"] = nextRun;
         command["ignoreRepeat"] = "true";
         tester->executeWaitVerifyContent(command);

         // Confirm nextRun is the given nextRun time, not in 1 hour
         SQResult result;
         tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
         ASSERT_EQUAL(result.size(), 1);
         ASSERT_EQUAL(result[0][0], nextRun);
    }

    // nextRun should take precedence over delay
    void hasDelayAndNextRun() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        const string nextRun = getTimeInFuture(10);
        command["nextRun"] = nextRun;
        command["delay"] = "900";
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun updated correctly
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], nextRun);
    }

    // Retry the job with HTTP
    void simpleRetryWithHttp() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob / HTTP/1.1";
        command["jobID"] = jobID;
        const string nextRun = getTimeInFuture(10);
        command["nextRun"] = nextRun;
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun updated correctly
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], nextRun);
    }

    string getTimeInFuture(int numSeconds) {
        time_t t = time(0);
        char buffer[26];
        t = t + numSeconds;
        struct tm* tm = localtime(&t);

        strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm);
        return buffer;
    }
} __RetryJobTest;
