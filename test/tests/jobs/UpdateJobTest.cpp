#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct UpdateJobTest : tpunit::TestFixture {
    UpdateJobTest()
            : tpunit::TestFixture("UpdateJob",
                                  BEFORE_CLASS(UpdateJobTest::setupClass),
                                  TEST(UpdateJobTest::updateJob),
                                  AFTER(UpdateJobTest::tearDown),
                                  AFTER_CLASS(UpdateJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(_threadID, {{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    // Simple UpdateJob with all parameters
    void updateJob() {
        // Create the job
        SData command("CreateJob");
        string jobName = "job";
        command["name"] = jobName;
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];
        ASSERT_GREATER_THAN(stol(jobID), 0);

        // Call the UpdateJob command
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        command["data"] = "{key:\"value\"}";
        command["repeat"] = "HOURLY";
        command["jobPriority"] = "1000";
        tester->executeWaitVerifyContent(command);

        // Verify that the job was actually updated
        SQResult currentJob;
        tester->readDB("SELECT repeat, data, priority FROM jobs WHERE jobID = " + jobID + ";", currentJob);
        ASSERT_EQUAL(currentJob[0][0], "HOURLY");
        ASSERT_EQUAL(currentJob[0][1], "{key:\"value\"}");
        ASSERT_EQUAL(currentJob[0][2], "1000");
    }

} __UpdateJobTest;

