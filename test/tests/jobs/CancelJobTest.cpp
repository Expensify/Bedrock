#include <libstuff/SQResult.h>
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct CancelJobTest : tpunit::TestFixture
{
    CancelJobTest()
        : tpunit::TestFixture("CancelJob",
                              BEFORE_CLASS(CancelJobTest::setupClass),
                              TEST(CancelJobTest::cancelNonExistentJob),
                              TEST(CancelJobTest::cancelJobWithChild),
                              TEST(CancelJobTest::cancelRunningJob),
                              TEST(CancelJobTest::cancelFinishedJob),
                              TEST(CancelJobTest::cancelPausedJob),
                              TEST(CancelJobTest::cancelChildJob),
                              TEST(CancelJobTest::cancelJobWithoutParent),
                              TEST(CancelJobTest::cancelJobWithSiblings),
                              AFTER(CancelJobTest::tearDown),
                              AFTER_CLASS(CancelJobTest::tearDownClass))
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

    // Cannot cancel a job that doesn't exist
    void cancelNonExistentJob()
    {
        SData command("CancelJob");
        command["jobID"] = "1";
        tester->executeWaitVerifyContent(command, "404 No job with this jobID");
    }

    // Cannot cancel a job with children
    void cancelJobWithChild()
    {
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

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get the child and finish it to put the parent in the QUEUED state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWaitVerifyContent(command);

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

        // Finish the known child.
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert parent is in QUEUED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Cannot finish a job with a child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command, "404 Invalid jobID - Cannot cancel a job with children");
    }

    // Ignore canceljob for RUNNING jobs
    void cancelRunningJob()
    {
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
        string jobID = response["jobID"];

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get the child job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWaitVerifyContent(command);

        // Assert job is in RUNNING state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Cannot finish a job in RUNNING state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
    }

    // Ignore canceljob for FINISHED jobs
    void cancelFinishedJob()
    {
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

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get the child and finish it to put the child in the FINISHED state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job is in FINISHED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "FINISHED");

        // Cannot finish a job in FINISHED state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "FINISHED");
    }

    // Ignore canceljob for PAUSED jobs
    void cancelPausedJob()
    {
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

        // Assert job is in PAUSED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");

        // Cannot finish a job in PAUSED state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");
    }

    // Cancel a child job
    void cancelChildJob()
    {
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

        // Create a sibling
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "sibling";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string siblingID = response["jobID"];

        // Finish the parent to put the children in the QUEUED state
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get one the children
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "sibling";
        tester->executeWaitVerifyContent(command);

        // Cancel the QUEUED child job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is cancelled, but sibling is still running
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "CANCELLED");

        tester->readDB("SELECT state FROM jobs WHERE jobID = " + siblingID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
    }

    // Cancel a child job that doesn't have a parent
    void cancelJobWithoutParent()
    {
        // Create the job
        SData command("CreateJob");
        command["name"] = "orphanChild";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Try cancelling the job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "404 Invalid jobID - Cannot cancel a job without a parent");
    }

    // Cancel a child job that's the last child job of the parent
    void cancelJobWithSiblings()
    {
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

        // Create one child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string childID = response["jobID"];

        // Create another child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child2";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string childID2 = response["jobID"];

        // Finish the parent to put the child in the QUEUED state
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Cancel one child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Parent should still be PAUSED
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

        // Cancel the last child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID2;
        tester->executeWaitVerifyContent(command);

        // Parent should be queued
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }
} __CancelJobTest;
