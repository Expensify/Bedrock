#include <test/lib/BedrockTester.h>

struct RequeueJobTest : tpunit::TestFixture {
    RequeueJobTest()
        : tpunit::TestFixture("RequeueJob",
                              BEFORE_CLASS(RequeueJobTest::setupClass),
                              //TEST(RequeueJobTest::nonExistentJob),
                              //TEST(RequeueJobTest::notInRunningState),
                              //TEST(RequeueJobTest::parentIsNotPaused),
                              TEST(RequeueJobTest::removeFinishedAndCancelledChildren),
                              //AFTER(RequeueJobTest::tearDown),
                              AFTER_CLASS(RequeueJobTest::tearDownClass)) { }

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
        SData command("RequeueJob");
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

        // Requeue it
        command.clear();
        command.methodLine = "RequeueJob";
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

        // Requeue the child
        command.clear();
        command.methodLine = "RequeueJob";
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
        command.methodLine = "CreateJob";
        command["name"] = "child_queued";
        command["parentJobID"] = parentID;
        response = getJsonResult(tester, command);
        string queuedChild = response["jobID"];

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWait(command);

        // Finish a child
        cout << "getting a child" << endl;
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child_finished";
        tester->executeWait(command);
        cout << "finishing a child" << endl;
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishedChildID;
        tester->executeWait(command);

        // Cancel a child
        cout << "cancel a child" << endl;
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = cancelledChildID;
        tester->executeWait(command);

        // Requeue the parent
        command.clear();
        command.methodLine = "RequeueJob";
        command["jobID"] = parentID;
        tester->executeWait(command);

        // check that only the queued child exists
    }
    // pass new data and make sure it updates the data
    // update the name and make sure that updates correctly
    // create with a negative delay
                    //throw "402 Must specify a non-negative delay when retrying";
    // create with a positive delay and confirm nextrun is updated appropriately
    // create with a nextrun string and confirm nextrun is updated appropriately

    void createchildflow() {
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

        // Finish the parent to put the child in the QUEUED state
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWait(command);

        // Get the child
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWait(command);

        // Requeue the child
        command.clear();
        command.methodLine = "RequeueJob";
        command["jobID"] = childID;
        tester->executeWait(command, "405 Can only requeue/finish child job when parent is PAUSED");
    }
    STable getJsonResult(BedrockTester* tester, SData command) {
        string resultJson = tester->executeWait(command);
        return SParseJSONObject(resultJson);
    }
} __RequeueJobTest;
/// copy all of these tests to a retry job as well
