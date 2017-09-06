#include <test/lib/BedrockTester.h>

struct GetJobTest : tpunit::TestFixture {
    GetJobTest()
        : tpunit::TestFixture("GetJob",
                              BEFORE_CLASS(GetJobTest::setupClass),
                              TEST(GetJobTest::getJob),
                              TEST(GetJobTest::withNumResults),
                              TEST(GetJobTest::noJobFound),
                              TEST(GetJobTest::testPriorities),
                              TEST(GetJobTest::testPrioritiesWithDifferentNextRunTimes),
                              TEST(GetJobTest::testWithFinishedAndCancelledChildren),
                              AFTER(GetJobTest::tearDown),
                              AFTER_CLASS(GetJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    // Simple GetJob
    void getJob() {
        // Create the job
        SData command("CreateJob");
        string jobName = "job";
        command["name"] = jobName;
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];
        ASSERT_GREATER_THAN(SToInt(jobID), 0);
        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + jobID + ";", originalJob);

        // GetJob
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = jobName;
        response = tester->executeWaitVerifyContentTable(command);

        ASSERT_EQUAL(response.size(), 3);
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);
        ASSERT_EQUAL(response["data"], "{}");

        // Check that nothing changed after we created the job except for the state and lastRun value
        SQResult currentJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + jobID + ";", currentJob);
        ASSERT_EQUAL(currentJob[0][0], originalJob[0][0]);
        ASSERT_EQUAL(currentJob[0][1], originalJob[0][1]);
        ASSERT_EQUAL(currentJob[0][2], "RUNNING");
        ASSERT_EQUAL(currentJob[0][3], jobName);
        ASSERT_EQUAL(currentJob[0][4], originalJob[0][4]);
        ASSERT_EQUAL(currentJob[0][5], SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow()));
        ASSERT_EQUAL(currentJob[0][6], originalJob[0][6]);
        ASSERT_EQUAL(currentJob[0][7], originalJob[0][7]);
        ASSERT_EQUAL(currentJob[0][8], originalJob[0][8]);
        ASSERT_EQUAL(currentJob[0][9], originalJob[0][9]);
    }

    // Cannot use numResults
    void withNumResults() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // GetJob
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        command["numResults"] = 20;
        tester->executeWaitVerifyContent(command, "402 Cannot use numResults with GetJob; try GetJobs");
    }

    // No job found
    void noJobFound() {
        // GetJob
        SData command("GetJob");
        command["name"] = "job";
        tester->executeWaitVerifyContent(command, "404 No job found");
    }

    // try creating a bunch of jobs at the same nextRun time but different priorities
    void testPriorities() {
        // Create jobs of different priorities
        // Low
        SData command("CreateJob");
        command["name"] = "low_5";
        command["priority"] = "0";
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobList;
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high_1";
        command["priority"] = "1000";
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium_3";
        command["priority"] = "500";
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high_2";
        command["priority"] = "1000";
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium_4";
        command["priority"] = "500";
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Confirm these jobs all have the same nextRun time
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID IN (" + SComposeList(jobList) + ") GROUP BY nextRun;", result);
        ASSERT_EQUAL(result.size(), 1);

        // GetJob and confrim that the jobs are returned in high, medium, low order
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "*";
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "high_1");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "high_2");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "medium_3");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "medium_4");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "low_5");
    }
    // Create jobs in order of low, medium, high, high, medium, low
    // with nextRun times in order of now, now+1, now+2, now+5, now+4, now+3
    // Expect the jobs to be returned in order of low, medium, high, low, medium, high
    void testPrioritiesWithDifferentNextRunTimes() {
        // Create jobs of different priorities
        // Low
        SData command("CreateJob");
        command["name"] = "low";
        command["priority"] = "0";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow());
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobList;
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium";
        command["priority"] = "500";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow()+1000000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high";
        command["priority"] = "1000";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow()+2000000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high";
        command["priority"] = "1000";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow()+5000000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium";
        command["priority"] = "500";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow()+4000000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Low
        command["name"] = "low";
        command["priority"] = "0";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow()+3000000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Confirm these jobs all have different nextRun times
        SQResult result;
        tester->readDB("SELECT nextRun FROM jobs WHERE jobID IN (" + SComposeList(jobList) + ") GROUP BY nextRun;", result);
        ASSERT_EQUAL(result.size(), 6);

        // GetJob and confrim that the first 3 jobs are returned in order of low, medium, high
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "*";
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "low");
        sleep(1);
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "medium");
        sleep(1);
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "high");

        // Sleep 3 seconds so that the nextRun time for the last 'high' job is now
        sleep(3);

        // GetJob and confirm that the last 3 jobs are returned in priority order since now is past nextRun for all of them
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "high");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "medium");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "low");
    }

    // get a parent job that has finished and cancelled jobs
    void testWithFinishedAndCancelledChildren() {
        // Create the parent
        SData command("CreateJob");
        command["name"] = "parent";
        string parentData = "{\"foo\":\"bar\"}";
        command["data"] = parentData;
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
        string finishedChildData = "{\"bar\":\"baz\"}";
        command["data"] = finishedChildData;
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string finishedChildID = response["jobID"];
        command.methodLine = "CreateJob";
        command["name"] = "child_cancelled";
        string cancelledChildData = "{\"baz\":\"foo\"}";
        command["data"] = cancelledChildData;
        response = tester->executeWaitVerifyContentTable(command);
        string cancelledChildID = response["jobID"];

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Cancel a child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = cancelledChildID;
        tester->executeWaitVerifyContent(command);

        // Get a child
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child_finished";
        response = tester->executeWaitVerifyContentTable(command);

        // Confirm the child has data about the parent in the response
        ASSERT_EQUAL(response.size(), 5);
        ASSERT_EQUAL(response["jobID"], finishedChildID);
        ASSERT_EQUAL(response["name"], "child_finished");
        ASSERT_EQUAL(response["data"], finishedChildData);
        ASSERT_EQUAL(response["parentJobID"], parentID);
        ASSERT_EQUAL(response["parentData"], parentData);

        // Finish a child
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishedChildID;
        tester->executeWaitVerifyContent(command);

        // Confirm the parent is set to QUEUED
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        response = tester->executeWaitVerifyContentTable(command);

        // Confirm data on the children are in the response
        ASSERT_EQUAL(response.size(), 5);
        ASSERT_EQUAL(response["jobID"], parentID);
        ASSERT_EQUAL(response["name"], "parent");
        ASSERT_EQUAL(response["data"], parentData);
        ASSERT_FALSE(response["finishedChildJobs"].empty());
        ASSERT_FALSE(response["cancelledChildJobs"].empty());

        // Checking the finished job
        list<string> finishedChildJobs = SParseJSONArray(response["finishedChildJobs"]);
        ASSERT_EQUAL(finishedChildJobs.size(), 1);
        STable childJob = SParseJSONObject(finishedChildJobs.front());
        ASSERT_EQUAL(childJob["jobID"], finishedChildID);
        ASSERT_EQUAL(childJob["data"], finishedChildData);

        // Checking the cancelled job
        list<string> cancelledChildJobs = SParseJSONArray(response["cancelledChildJobs"]);
        ASSERT_EQUAL(cancelledChildJobs.size(), 1);
        childJob = SParseJSONObject(cancelledChildJobs.front());
        ASSERT_EQUAL(childJob["jobID"], cancelledChildID);
        ASSERT_EQUAL(childJob["data"], cancelledChildData);
    }
} __GetJobTest;

