#include <test/lib/BedrockTester.h>
#include <plugins/Jobs.h>

struct DeleteJobTest : tpunit::TestFixture {
    DeleteJobTest()
        : tpunit::TestFixture("DeleteJob",
                              BEFORE_CLASS(DeleteJobTest::setupClass),
                              TEST(DeleteJobTest::deleteNonExistentJob),
                              TEST(DeleteJobTest::deleteJobWithChild),
                              TEST(DeleteJobTest::deleteRunningJob),
                              TEST(DeleteJobTest::deleteFinishedJob),
                              AFTER(DeleteJobTest::tearDown),
                              AFTER_CLASS(DeleteJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(_threadID, {{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        for (int64_t i = 0; i < BedrockPlugin_Jobs::TABLE_COUNT; i++) {
            string tableName = BedrockPlugin_Jobs::getTableName(i);
            command["query"] = "DELETE FROM " + tableName + " WHERE jobID > 0;";
            tester->executeWaitVerifyContent(command);
        }
    }

    void tearDownClass() { delete tester; }

    // Cannot delete a job that doesn't exist
    void deleteNonExistentJob() {
        SData command("DeleteJob");
        command["jobID"] = "1";
        tester->executeWaitVerifyContent(command, "404 No job with this jobID");
    }

    // Cannot delete a job with children
    void deleteJobWithChild() {
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

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

        // Assert parent is in PAUSED state
        SQResult result;
        string tableName = BedrockPlugin_Jobs::getTableName(stol(parentID));
        tester->readDB("SELECT state FROM " + tableName + " WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");

        // Cannot finish a job with a child
        command.clear();
        command.methodLine = "DeleteJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command, "405 Can't delete a parent job with children running");
    }

    // Ignore deletejob for RUNNING jobs
    void deleteRunningJob() {
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
        string tableName = BedrockPlugin_Jobs::getTableName(stol(jobID));
        tester->readDB("SELECT state FROM " + tableName + " WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Cannot finish a job in RUNNING state
        command.clear();
        command.methodLine = "DeleteJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can't delete a RUNNING job");

        // Assert job state is unchanged
        tableName = BedrockPlugin_Jobs::getTableName(stol(jobID));
        tester->readDB("SELECT state FROM " + tableName + " WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
    }

    // Delete finished jobs
    void deleteFinishedJob() {
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
        string tableName = BedrockPlugin_Jobs::getTableName(stol(childID));
        tester->readDB("SELECT state FROM " + tableName + " WHERE jobID = " + childID + ";", result);
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
} __DeleteJobTest;
