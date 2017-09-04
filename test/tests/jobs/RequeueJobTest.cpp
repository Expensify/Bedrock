#include <test/lib/BedrockTester.h>

struct RequeueJobTest : tpunit::TestFixture {
    RequeueJobTest()
        : tpunit::TestFixture("RequeueJob",
                              BEFORE_CLASS(RequeueJobTest::setupClass),
                              TEST(RequeueJobTest::nonExistentJob),
                              TEST(RequeueJobTest::notInRunningState),
                              TEST(RequeueJobTest::parentIsNotPaused),
                              TEST(RequeueJobTest::removeFinishedAndCancelledChildren),
                              TEST(RequeueJobTest::nonExistentJob),
                              TEST(RequeueJobTest::updateData),
                              TEST(RequeueJobTest::withDelay),
                              TEST(RequeueJobTest::hasRepeat),
                              TEST(RequeueJobTest::noNextRun),
                              TEST(RequeueJobTest::simplyRequeue),
                              TEST(RequeueJobTest::changeName),
                              AFTER(RequeueJobTest::tearDown),
                              AFTER_CLASS(RequeueJobTest::tearDownClass)) { }

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
        SData command("RequeueJob");
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
        command.methodLine = "RequeueJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can only requeue/finish RUNNING jobs");
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
        command.methodLine = "RequeueJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command, "405 Can only requeue/finish child job when parent is PAUSED");
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

        // Retry the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "RequeueJob";
        command["jobID"] = parentID;
        command["nextRun"] = getTimeInFuture(10);
        tester->executeWaitVerifyContent(command);

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
        command.methodLine = "RequeueJob";
        command["jobID"] = jobID;
        command["data"] = SComposeJSONObject(data);
        command["nextRun"] = getTimeInFuture(10);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT data FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], SComposeJSONObject(data));
    }

    // Cannot requeue with a delay
    void withDelay() {
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
        command.methodLine = "RequeueJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWaitVerifyContent(command, "402 Cannot requeue job, no nextRun is set");
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
        command.methodLine = "RequeueJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

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

    void noNextRun() {
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
        command.methodLine = "RequeueJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "402 Cannot requeue job, no nextRun is set");
    }

    // Confirm nextrun is updated appropriately
    void simplyRequeue() {
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
        command.methodLine = "RequeueJob";
        command["jobID"] = jobID;
        const string nextRun = getTimeInFuture(10);
        command["nextRun"] = nextRun;
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], nextRun);
    }

    // Update the name
    void changeName() {
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
        command.methodLine = "RequeueJob";
        command["jobID"] = jobID;
        command["name"] = "newName";
        command["nextRun"] = getTimeInFuture(10);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        tester->readDB("SELECT name FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "newName");
    }

    string getTimeInFuture(int numSeconds) {
        time_t t = time(0);
        char buffer[26];
        t = t + numSeconds;
        struct tm* tm = localtime(&t);

        strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm);
        return buffer;
    }
} __RequeueJobTest;
