#include <test/lib/BedrockTester.h>

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
                              TEST(RetryJobTest::hasRepeat),
                              AFTER(RetryJobTest::tearDown),
                              AFTER_CLASS(RetryJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(); }

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWait(command);
    }

    void tearDownClass() { delete tester; }

    // Throw an error if the job doesn't exist
    void nonExistentJob() {
        SData command("RetryJob");
        command["jobID"] = "1";
        tester->executeWait(command, "404 No job with this jobID");
    }

    // Throw an error if the job is not in RUNNING state
    void notInRunningState() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = getJsonResult(tester, command);
        string jobID = response["jobID"];

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        tester->executeWait(command, "405 Can only requeue/finish RUNNING jobs");
    }

    // If job has a parentID, the parent should be paused
    void parentIsNotPaused() {
        // Create the parent
        SData command("CreateJob");
        command["name"] = "parent";
        STable response = getJsonResult(tester, command);
        string parentID = response["jobID"];

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWait(command);

        // Create the child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child";
        command["parentJobID"] = parentID;
        response = getJsonResult(tester, command);
        string childID = response["jobID"];

        // It's not possible to put the child in the QUEUED state without the parent being paused
        // and a child cannot being the RUNNING state without first being the QUEUED state
        // but we check for this to make sure something funky didn't occur.
        // We'll manually put the child in the RUNNING state to hit this condition
        command.clear();
        command.methodLine = "Query";
        command["query"] = "UPDATE jobs SET state = 'RUNNING' WHERE jobID = " + childID + ";";
        tester->executeWait(command);

        // Retry the child
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = childID;
        tester->executeWait(command, "405 Can only requeue/finish child job when parent is PAUSED");
    }

    // Child jobs that are in the FINISHED or CANCELLED state should be deleted when the parent is finished
    void removeFinishedAndCancelledChildren() {
        // Create the parent
        SData command("CreateJob");
        command["name"] = "parent";
        STable response = getJsonResult(tester, command);
        string parentID = response["jobID"];

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWait(command);

        // Create the children
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_finished";
        command["parentJobID"] = parentID;
        response = getJsonResult(tester, command);
        string finishedChildID = response["jobID"];
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_cancelled";
        command["parentJobID"] = parentID;
        response = getJsonResult(tester, command);
        string cancelledChildID = response["jobID"];
        command.clear();

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWait(command);

        // Cancel a child
        // if this goes 2nd this doesn't requeue the parent job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = cancelledChildID;
        tester->executeWait(command);

        // Finish a child
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child_finished";
        tester->executeWait(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishedChildID;
        tester->executeWait(command);

        /* I don't know how to get a child in the PAUSED state, so ignoring this for now
        // Create a grandchild and finish a child so the child in state PAUSED
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_paused";
        command["parentJobID"] = parentID;
        response = getJsonResult(tester, command);
        string pausedChild = response["jobID"];
        command.methodLine = "GetJob";
        command["name"] = "child_paused";
        tester->executeWait(command);
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "grandchild";
        command["parentJobID"] = pausedChild;
        response = getJsonResult(tester, command);
        string grandchild = response["jobID"];
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = pausedChild;
        tester->executeWait(command);
        */

        // Retry the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWait(command);
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = parentID;
        tester->executeWait(command);

        // Confirm that the FINISHED and CANCELLED children are deleted
        SQResult result;
        tester->readDB("SELECT count(*) FROM jobs WHERE jobID != " + parentID + ";", result);
        ASSERT_EQUAL(SToInt(result[0][0]), 0);
    }

    // Update the job data if new data is passed
    void updateData() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = getJsonResult(tester, command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWait(command);

        // Retry it
        STable data;
        data["foo"] = "bar";
        data["bar"] = "foo";
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["data"] = SComposeJSONObject(data);
        tester->executeWait(command);

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
        STable response = getJsonResult(tester, command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWait(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["delay"] = "-5";
        tester->executeWait(command, "402 Must specify a non-negative delay when retrying");
    }

    // Retry with a positive delay and confirm nextRun is updated appropriately
    void positiveDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = getJsonResult(tester, command);
        string jobID = response["jobID"];

        // Get the nextRun value
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        string originalNextRun = result[0][0];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWait(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWait(command);

        // Assert the new nextRun value is correct
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        string currentNextRun = result[0][0];
        struct tm tm1;
        struct tm tm2;
        strptime(originalNextRun.c_str(), "%Y-%m-%d %H:%M:%S", &tm1);
        time_t originalNextRunTime = mktime(&tm1);
        strptime(currentNextRun.c_str(), "%Y-%m-%d %H:%M:%S", &tm2);
        time_t currentNextRunTime = mktime(&tm2);
        ASSERT_EQUAL(difftime(currentNextRunTime, originalNextRunTime), 5);
    }

    // Retry a job with a repeat
    void hasRepeat() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = getJsonResult(tester, command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWait(command);

        // Retry it
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = jobID;
        tester->executeWait(command);

        // Confirm nextRun is in 1 hour
        SQResult result;
        tester->readDB("SELECT created, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        struct tm tm1;
        struct tm tm2;
        strptime(result[0][0].c_str(), "%Y-%m-%d %H:%M:%S", &tm1);
        time_t createdTime = mktime(&tm1);
        strptime(result[0][1].c_str(), "%Y-%m-%d %H:%M:%S", &tm2);
        time_t nextRunTime = mktime(&tm2);
        ASSERT_EQUAL(difftime(nextRunTime, createdTime), 3600);
    }

    STable getJsonResult(BedrockTester* tester, SData command) {
        string resultJson = tester->executeWait(command);
        return SParseJSONObject(resultJson);
    }
} __RetryJobTest;
