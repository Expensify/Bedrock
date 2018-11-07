#include <test/lib/BedrockTester.h>

struct GetJobTest : tpunit::TestFixture {
    GetJobTest()
        : tpunit::TestFixture("GetJob",
                              BEFORE_CLASS(GetJobTest::setupClass),
                              TEST(GetJobTest::getJob),
                              TEST(GetJobTest::getJobWithHttp),
                              TEST(GetJobTest::withNumResults),
                              TEST(GetJobTest::noJobFound),
                              TEST(GetJobTest::testPriorities),
                              TEST(GetJobTest::testPrioritiesWithDifferentNextRunTimes),
                              TEST(GetJobTest::testWithFinishedAndCancelledChildren),
                              TEST(GetJobTest::testPrioritiesWithRunQueued),
                              TEST(GetJobTest::testMultipleNames),
                              AFTER(GetJobTest::tearDown),
                              AFTER_CLASS(GetJobTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(_threadID, {{"-plugins", "Jobs,DB"}}, {});}

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

        ASSERT_EQUAL(response.size(), 4);
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);
        ASSERT_EQUAL(response["data"], "{}");
        SASSERT(!response["created"].empty());

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

    // Simple GetJob with Http
    void getJobWithHttp() {
        uint64_t start = STimeNow();
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
        command.methodLine = "GetJob / HTTP/1.1";
        command["name"] = jobName;
        response = tester->executeWaitVerifyContentTable(command);

        uint64_t end = STimeNow();

        ASSERT_EQUAL(response.size(), 4);
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);
        ASSERT_EQUAL(response["data"], "{}");
        SASSERT(!response["created"].empty());

        // Check that nothing changed after we created the job except for the state and lastRun value
        SQResult currentJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + jobID + ";", currentJob);
        ASSERT_EQUAL(currentJob[0][0], originalJob[0][0]);
        ASSERT_EQUAL(currentJob[0][1], originalJob[0][1]);
        ASSERT_EQUAL(currentJob[0][2], "RUNNING");
        ASSERT_EQUAL(currentJob[0][3], jobName);
        ASSERT_EQUAL(currentJob[0][4], originalJob[0][4]);

        // The lastRun time can be anything from start to end, inclusive.
        bool saneRunTime = false;
        uint64_t testTime = start;
        while (true) {
            string testTimeString = SComposeTime("%Y-%m-%d %H:%M:%S", testTime);
            if (currentJob[0][5] == testTimeString) {
                saneRunTime = true;
                break;
            }
            if (testTime == end) {
                // We already tried everything.
                break;
            }
            testTime += 1'000'000; // next second.
            if (testTime > end) {
                // this is the last possible test.
                testTime = end;
            }
        }
        ASSERT_TRUE(saneRunTime);

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

    // Create jobs with the same nextRun time but different priorities
    void testPriorities() {
        string firstRun = SComposeTime("%Y-%m-%d %H:%M:%S", STimeNow());
        // Create jobs of different priorities
        // Low
        SData command("CreateJob");
        command["name"] = "low_5";
        command["priority"] = "0";
        command["firstRun"] = firstRun;
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobList;
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high_1";
        command["priority"] = "1000";
        command["firstRun"] = firstRun;
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium_3";
        command["priority"] = "500";
        command["firstRun"] = firstRun;
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high_2";
        command["priority"] = "1000";
        command["firstRun"] = firstRun;
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium_4";
        command["priority"] = "500";
        command["firstRun"] = firstRun;
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Confirm these jobs all have the same nextRun time
        SQResult result;
        tester->readDB("SELECT DISTINCT nextRun FROM jobs WHERE jobID IN (" + SComposeList(jobList) + ");", result);
        ASSERT_EQUAL(result.size(), 1);

        // GetJob and confirm that the jobs are returned in high, medium, low order
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
    // Expect the jobs to be returned in order of low, medium, high, high, medium, low
    void testPrioritiesWithDifferentNextRunTimes() {

        // We mark a `start at` time because timing is critical to this test.
        uint64_t startAt = STimeNow();
        // Create jobs of different priorities
        // Low
        SData command("CreateJob");
        command["name"] = "low";
        command["priority"] = "0";
        const uint64_t time = STimeNow();
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time + 5'000'000);
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobList;
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium";
        command["priority"] = "500";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time + 6'000'000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high";
        command["priority"] = "1000";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time + 7'000'000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // High
        command["name"] = "high";
        command["priority"] = "1000";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time + 10'000'000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Medium
        command["name"] = "medium";
        command["priority"] = "500";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time + 9'000'000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Low
        command["name"] = "low";
        command["priority"] = "0";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time + 8'000'000);
        response = tester->executeWaitVerifyContentTable(command);
        jobList.push_back(response["jobID"]);

        // Confirm these jobs all have different nextRun times
        SQResult result;
        tester->readDB("SELECT DISTINCT nextRun FROM jobs WHERE jobID IN (" + SComposeList(jobList) + ");", result);
        ASSERT_EQUAL(result.size(), 6);

        // Make sure we finished this fast enough that we can still dequeue commands in the order we expect.
        // We require the above to have finished in 3 seconds or less.
        uint64_t createdBy = STimeNow();
        ASSERT_LESS_THAN(createdBy, startAt + 3'000'000);

        // Now we sleep until it's nearly (but not too close) time to get the first command. This will sleep the
        // remainder of the three seconds above.
        usleep(3'000'000 - (createdBy - startAt));

        // Get jobs in the order they become available. Make sure the first three we get are in the order low, medium,
        // high, as that's when they were scheduled to run. This test can fail if timing is off, as we expect
        // everything to happen correctly over 1-second intervals.
        vector<string> names;

        // Now we allow 10 seconds to get all the commands. This is 13 seconds from the start, giving us a 3-second
        // margin.
        uint64_t timeout = STimeNow() + 10'000'000;
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "*";
        list<STable> responses;
        while (true) {
            try {
                response = tester->executeWaitVerifyContentTable(command);
                names.push_back(response["name"]);
                responses.push_back(response);
                if (names.size() == 3) {
                    // Done, did all three.
                    break;
                }
                if (STimeNow() > timeout) {
                    // Took too long.
                    break;
                }
                // Wait 50 ms and try again.
                usleep(50'000);
            } catch (const SException& e) {
                if (SContains(e.what(), "404 No job found")) {
                    // This is fine, we expect this, just try again.
                } else {
                    throw;
                }
            }
        }

        // Now we should have three responses verify they're correct.
        ASSERT_EQUAL(names.size(), 3);

        if (names[0] != "low" || names[1] != "medium" || names[2] != "high") {
            cout << "Will fail:" << endl;
            for (auto& a : responses) {
                for (auto&p : a) {
                    cout << p.first << ": " << p.second << endl;
                }
                cout << endl;
            }
        }
        ASSERT_EQUAL(names[0], "low");
        ASSERT_EQUAL(names[1], "medium");
        ASSERT_EQUAL(names[2], "high");

        // GetJob and confirm that the last 3 jobs are returned in priority order since now is past nextRun for all of them
        sleep(5);
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "high");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "medium");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "low");
    }

    // Get a parent job that has finished and cancelled jobs
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
        
        // Get a child
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child_finished";
        response = tester->executeWaitVerifyContentTable(command);
        
        // Cancel a child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = cancelledChildID;
        tester->executeWaitVerifyContent(command);

        // Confirm the child has data about the parent in the response
        ASSERT_EQUAL(response.size(), 6);
        ASSERT_EQUAL(response["jobID"], finishedChildID);
        ASSERT_EQUAL(response["name"], "child_finished");
        ASSERT_EQUAL(response["data"], finishedChildData);
        ASSERT_EQUAL(response["parentJobID"], parentID);
        ASSERT_EQUAL(response["parentData"], parentData);

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

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
        ASSERT_EQUAL(response.size(), 6);
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

    // This is the same as testPriorities but some of the states are set to RUNQUEUED
    // We also change one of the high priority jobs to run a second earlier than the others to
    // test that RUNQUEUED jobs will run before QUEUED jobs when the RUNQUEUED nextRun is earlier
    // than the QUEUED nextRun
    // We set the firstRun for all the jobs that don't have a retryAfter to 2 seconds in the future
    // This way, after the two jobs with retryAfter are run, the nextRun will be the same for all jobs but one
    void testPrioritiesWithRunQueued() {
        // Create jobs of different priorities
        // Low
        SData command("CreateJob");
        command["name"] = "low_5";
        command["priority"] = "0";
        const uint64_t time = STimeNow();
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time+2000000);
        tester->executeWaitVerifyContent(command);

        // High
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "high_1";
        command["priority"] = "1000";
        command["retryAfter"] = "+1 SECONDS";
        tester->executeWaitVerifyContent(command);

        // Medium
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "medium_4";
        command["priority"] = "500";
        command["retryAfter"] = "+2 SECONDS";
        tester->executeWaitVerifyContent(command);

        // High
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "high_2";
        command["priority"] = "1000";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time+2000000);
        tester->executeWaitVerifyContent(command);

        // Medium
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "medium_3";
        command["priority"] = "500";
        command["firstRun"] = SComposeTime("%Y-%m-%d %H:%M:%S", time+2000000);
        tester->executeWaitVerifyContent(command);

        // Get the two jobs with retryAfter set to put them in a RUNQUEUED state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "high_1";
        tester->executeWaitVerifyContent(command);
        command["name"] = "medium_4";
        tester->executeWaitVerifyContent(command);

        // Confirm they are in the RUNQUEUED state
        SQResult result;
        tester->readDB("SELECT DISTINCT state FROM jobs WHERE name IN ('high_1', 'medium_4') AND JSON_EXTRACT(data, '$.mockRequest') IS NULL;", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_EQUAL(result[0][0], "RUNQUEUED");

        // Sleep for two seconds and then confirm that all jobs but high_1 have the same nextRun time
        sleep(2);
        tester->readDB("SELECT DISTINCT nextRun, GROUP_CONCAT(name) FROM jobs WHERE JSON_EXTRACT(data, '$.mockRequest') IS NULL GROUP BY nextRun;", result);
        ASSERT_EQUAL(result.size(), 2);
        ASSERT_EQUAL(SParseList(result[0][1]).size(), 1);
        ASSERT_EQUAL(result[0][1], "high_1");
        ASSERT_EQUAL(SParseList(result[1][1]).size(), 4);

        // GetJob and confirm that the jobs are returned in high, medium, low order
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "*";
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "high_1");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "high_2");
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_TRUE(response["name"] == "medium_4" || response["name"] == "medium_3"); // Because we don't order by jobID, the order of these jobs depends on the table/index used to retrieve them
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_TRUE(response["name"] == "medium_4" || response["name"] == "medium_3"); // Because we don't order by jobID, the order of these jobs depends on the table/index used to retrieve them
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["name"], "low_5");
    }

    // Get a job from a list of names and make sure the jobs are returned in the proper order by priorities
    void testMultipleNames() {
        // Create jobs of different priorities
        // Low
        SData command("CreateJob");
        command["name"] = "low";
        command["priority"] = "0";
        tester->executeWaitVerifyContent(command);

        // High
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "high";
        command["priority"] = "1000";
        tester->executeWaitVerifyContent(command);

        // Medium
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "medium";
        command["priority"] = "500";
        tester->executeWaitVerifyContent(command);

        // Medium
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "medium";
        command["priority"] = "500";
        tester->executeWaitVerifyContent(command);

        // Get medium and high jobs
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "medium,high";
        tester->executeWaitVerifyContent(command);

        // Confirm that high is in the RUNNING state
        SQResult result;
        tester->readDB("SELECT DISTINCT state FROM jobs WHERE name = 'high' AND JSON_EXTRACT(data, '$.mockRequest') IS NULL;", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Get any of the job names
        command["name"] = "low,medium,high";
        tester->executeWaitVerifyContent(command);

        // Confirm that medium is in the RUNNING state since there are no more high jobs
        tester->readDB("SELECT COUNT(1) FROM jobs WHERE name = 'medium' AND state = 'RUNNING' AND JSON_EXTRACT(data, '$.mockRequest') IS NULL;", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_EQUAL(result[0][0], "1");

        // Return 2 jobs and confirm that the jobs are returned in medium and low order
        command.clear();
        command.methodLine = "GetJobs";
        command["name"] = "low,medium";
        command["numResults"] = "2";

        tester->readDB("SELECT COUNT(1) FROM jobs WHERE name = 'medium' AND state = 'RUNNING' AND JSON_EXTRACT(data, '$.mockRequest') IS NULL;", result);
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobList = SParseJSONArray(response["jobs"]);
        ASSERT_EQUAL(jobList.size(), 2);
    }
} __GetJobTest;

