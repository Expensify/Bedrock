#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>

struct DeleteJobTest : tpunit::TestFixture
{
    DeleteJobTest()
        : tpunit::TestFixture("DeleteJob",
                              BEFORE_CLASS(DeleteJobTest::setupClass),
                              TEST(DeleteJobTest::deleteNonExistentJob),
                              TEST(DeleteJobTest::deleteJobWithChild),
                              TEST(DeleteJobTest::deleteRunningJob),
                              TEST(DeleteJobTest::deleteFinishedJob),
                              TEST(DeleteJobTest::deleteJobPromotesWaitingJob),
                              AFTER(DeleteJobTest::tearDown),
                              AFTER_CLASS(DeleteJobTest::tearDownClass))
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

    // Cannot delete a job that doesn't exist
    void deleteNonExistentJob()
    {
        SData command("DeleteJob");
        command["jobID"] = "1";
        tester->executeWaitVerifyContent(command, "404 No job with this jobID");
    }

    // Cannot delete a job with children
    void deleteJobWithChild()
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

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

        // Assert parent is in PAUSED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");

        // Cannot finish a job with a child
        command.clear();
        command.methodLine = "DeleteJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command, "405 Can't delete a parent jobs with children running");
    }

    // Ignore deletejob for RUNNING jobs
    void deleteRunningJob()
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
        command.methodLine = "DeleteJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can't delete a RUNNING job");

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
    }

    // Delete finished jobs
    void deleteFinishedJob()
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

        // Delete the child job
        command.clear();
        command.methodLine = "DeleteJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Delete the parent job
        command.clear();
        command.methodLine = "DeleteJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);
    }

    // DeleteJob should promote the next WAITING job with same sequentialKey
    void deleteJobPromotesWaitingJob()
    {
        // Create first job
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_delete";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        // Create second job
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_delete";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        // Verify second job is WAITING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");

        // Delete first job
        command.clear();
        command.methodLine = "DeleteJob";
        command["jobID"] = jobID1;
        tester->executeWaitVerifyContent(command);

        // Verify first job is deleted
        tester->readDB("SELECT * FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_TRUE(result.empty());

        // Verify second job is now QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }
} __DeleteJobTest;
