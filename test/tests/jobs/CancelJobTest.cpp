#include <test/lib/BedrockTester.h>
#include <test/Utils.h>

struct CancelJobTest : tpunit::TestFixture {
    CancelJobTest()
        : tpunit::TestFixture("CancelJob",
                              BEFORE_CLASS(CancelJobTest::setupClass),
                              TEST(CancelJobTest::cancelNonExistentJob),
                              TEST(CancelJobTest::cancelJobWithChild),
                              TEST(CancelJobTest::cancelRunningJob),
                              TEST(CancelJobTest::cancelFinishedJob),
                              TEST(CancelJobTest::cancelPausedJob),
                              TEST(CancelJobTest::cancelJob),
                              TEST(CancelJobTest::cancelChildJob),
                              AFTER(CancelJobTest::tearDown),
                              AFTER_CLASS(CancelJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(); }

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWait(command);
    }

    void tearDownClass() { delete tester; }

    // Cannot cancel a job that doesn't exist
    void cancelNonExistentJob() {
        SData command("CancelJob");
        command["jobID"] = "1";
        tester->executeWait(command, "404 No job with this jobID");
    }

    // Cannot cancel a job with children
    void cancelJobWithChild() {
        // Create a parent job
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

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWait(command);

        // Get the child and finish it to put the parent in the QUEUED state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWait(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = childID;
        tester->executeWait(command);

        // Assert parent is in QUEUED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Cannot finish a job with a child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = parentID;
        tester->executeWait(command, "404 Invalid jobID - Cannot cancel a job with children");
    }

    // Ignore canceljob for RUNNING jobs
    void cancelRunningJob() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = getJsonResult(tester, command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWait(command);

        // Assert job is in RUNNING state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Cannot finish a job in RUNNING state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = jobID;
        tester->executeWait(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
    }

    // Ignore canceljob for FINISHED jobs
    void cancelFinishedJob() {
        // Create a parent job
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

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWait(command);

        // Get the child and finish it to put the child in the FINISHED state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWait(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = childID;
        tester->executeWait(command);

        // Assert job is in FINISHED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "FINISHED");

        // Cannot finish a job in FINISHED state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWait(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "FINISHED");
    }

    // Ignore canceljob for PAUSED jobs
    void cancelPausedJob() {
        // Create a parent job
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

        // Assert job is in PAUSED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");

        // Cannot finish a job in PAUSED state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWait(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");
    }

    // Cancel a job
    void cancelJob() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = getJsonResult(tester, command);
        string jobID = response["jobID"];

        // Cancel it
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = jobID;
        tester->executeWait(command);

        // Assert job state is cancelled
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "CANCELLED");
    }

    // Cancel a child job
    void cancelChildJob() {
        // Create a parent job
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

        // Cancel the child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWait(command);

        // Assert job state is cancelled
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "CANCELLED");
    }
} __CancelJobTest;
