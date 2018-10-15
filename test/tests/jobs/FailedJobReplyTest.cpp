#include <test/lib/BedrockTester.h>

struct FailedJobReplyTest : tpunit::TestFixture {
    FailedJobReplyTest()
        : tpunit::TestFixture("FailedJobReply",
                              BEFORE_CLASS(FailedJobReplyTest::setupClass),
                              TEST(FailedJobReplyTest::failSendingResponse),
                              AFTER(FailedJobReplyTest::tearDown),
                              AFTER_CLASS(FailedJobReplyTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(_threadID, {{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    // Cannot cancel a job with children
    void failSendingResponse() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "willFailReply";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Create a GetJobs command that will run in the future, once we've disconnected.
        command.methodLine = "GetJob";
        command["name"] = "willFailReply";
        command["commandExecuteTime"] = to_string(STimeNow() + 1000000);
        response = tester->executeWaitVerifyContentTable(command, "202");

        // Wait for the command to run.
        sleep(2);

        // Try again, we should get the job because it was queued, not fail because it was RUNNING.
        command.nameValueMap.clear();
        command["name"] = "willFailReply";
        response = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response["jobID"];

        ASSERT_EQUAL(jobID, jobID2);
    }
} __FailedJobReplyTest;
