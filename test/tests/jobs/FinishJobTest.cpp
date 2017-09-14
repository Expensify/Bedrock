#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/Utils.h>

struct FinishJobTest : tpunit::TestFixture {
    FinishJobTest()
        : tpunit::TestFixture("FinishJob",
                              BEFORE_CLASS(FinishJobTest::setupClass),
                              TEST(FinishJobTest::nonExistentJob),
                              TEST(FinishJobTest::notInRunningState),
                              TEST(FinishJobTest::parentIsNotPaused),
                              TEST(FinishJobTest::removeFinishedAndCancelledChildren),
                              TEST(FinishJobTest::updateData),
                              TEST(FinishJobTest::finishingParentUnPausesChildren),
                              TEST(FinishJobTest::deleteFinishedJobWithNoChildren),
                              TEST(FinishJobTest::hasRepeat),
                              AFTER(FinishJobTest::tearDown),
                              AFTER_CLASS(FinishJobTest::tearDownClass)) { }

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
        SData command("FinishJob");
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

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can only retry/finish RUNNING jobs");
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

        // Finish the child
        command.clear();
        command.methodLine = "FinishJob";
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

        // Cancel a child
        // if this goes 2nd this doesn't requeue the parent job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = cancelledChildID;
        tester->executeWaitVerifyContent(command);

        // Finish a child
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child_finished";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishedChildID;
        tester->executeWaitVerifyContent(command);

        // Confirm the parent is set to QUEUED
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Finish the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Confirm that the FINISHED and CANCELLED children are deleted
        tester->readDB("SELECT count(*) FROM jobs WHERE jobID != " + parentID + ";", result);
        ASSERT_EQUAL(SToInt(result[0][0]), 0);
    }

    // Update the job data if new data is passed
    void updateData() {
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

        // Finish it
        STable data;
        data["foo"] = "bar";
        data["bar"] = "foo";
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["data"] = SComposeJSONObject(data);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT data FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], SComposeJSONObject(data));
    }

    void finishingParentUnPausesChildren() {
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

        // Confirm that the parent is in the PAUSED state and the chilrden are in the QUEUED state
        SQResult result;
        tester->readDB("SELECT jobID, state FROM jobs;", result);
        ASSERT_EQUAL(result[0][0], parentID);
        ASSERT_EQUAL(result[0][1], "PAUSED");
        ASSERT_EQUAL(result[1][0], finishedChildID);
        ASSERT_EQUAL(result[1][1], "QUEUED");
        ASSERT_EQUAL(result[2][0], cancelledChildID);
        ASSERT_EQUAL(result[2][1], "QUEUED");
    }

    void deleteFinishedJobWithNoChildren() {
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

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confirm the job was deleted
        SQResult result;
        tester->readDB("SELECT * FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_TRUE(result.empty());
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

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["delay"] = "-5";
        tester->executeWaitVerifyContent(command, "402 Must specify a non-negative delay when retrying");
    }

    // Finish with a positive delay and confirm nextRun is updated appropriately
    void positiveDelay() {
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

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWaitVerifyContent(command);

        // Assert the new nextRun time is 5 seconds after the original nextRun time
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        time_t currentNextRunTime = getTimestampForDateTimeString(result[0][0]);
        time_t originalNextRunTime = getTimestampForDateTimeString(originalNextRun);
        ASSERT_EQUAL(difftime(currentNextRunTime, originalNextRunTime), 5);
    }

    // Finish a job with a repeat
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

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is in 1 hour from the created time
        SQResult result;
        tester->readDB("SELECT created, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        time_t createdTime = getTimestampForDateTimeString(result[0][0]);
        time_t nextRunTime = getTimestampForDateTimeString(result[0][1]);
        ASSERT_EQUAL(difftime(nextRunTime, createdTime), 3600);
    }

} __FinishJobTest;
