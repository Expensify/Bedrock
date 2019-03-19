#include <test/lib/BedrockTester.h>

struct QueryJobsTest : tpunit::TestFixture {
    QueryJobsTest ()
            : tpunit::TestFixture("QueryJob",
                                  BEFORE_CLASS(QueryJobsTest::setupClass),
                                  TEST(QueryJobsTest::queryJob),
                                  AFTER(QueryJobsTest::tearDown),
                                  AFTER_CLASS(QueryJobsTest::tearDownClass)) { }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester(_threadID, {{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    void queryJob() {
        // Create the job
        SData command("CreateJob");
        string jobName = "job";
        command["name"] = jobName;
        command["firstRun"] = "2042-04-02 00:42:42";
        command["repeat"] = "FINISHED, +1 DAY";
        command["data"] = "{\"something\":1}";
        command["priority"] = "1000";
        STable response = tester->executeWaitVerifyContentTable(command);
        const string& jobID1 = response["jobID"];
        ASSERT_GREATER_THAN(stol(jobID1), 0);
        list<string> jobIDs;
        jobIDs.push_back(jobID1);

        command["name"] = jobName;
        command["firstRun"] = "2042-04-03 00:42:43";
        command["repeat"] = "FINISHED, +2 DAY";
        command["data"] = "{\"something\":2}";
        command["priority"] = "500";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        const string& jobID2 = response2["jobID"];
        ASSERT_GREATER_THAN(stol(jobID2), 0);
        jobIDs.push_back(jobID2);

        command.clear();
        command.methodLine = "QueryJobs";
        command["jobIDList"] = SComposeList(jobIDs);
        STable response3 = tester->executeWaitVerifyContentTable(command);
        list<string> jobList = SParseJSONArray(response3["jobs"]);
        ASSERT_EQUAL(jobList.size(), 2);
        for (auto job : jobList) {
            auto parsed = SParseJSONObject(job);
            if (parsed["jobID"] == jobID1) {
                ASSERT_EQUAL(parsed["name"], jobName);
                ASSERT_EQUAL(parsed["nextRun"], "2042-04-02 00:42:42");
                ASSERT_EQUAL(parsed["repeat"], "FINISHED, +1 DAY");
                ASSERT_EQUAL(parsed["data"], "{\"something\":1}");
                ASSERT_EQUAL(parsed["priority"], "1000");
                SASSERT(!parsed["created"].empty());
            } else {
                ASSERT_EQUAL(parsed["jobID"], jobID2);
                ASSERT_EQUAL(parsed["name"], jobName);
                ASSERT_EQUAL(parsed["nextRun"], "2042-04-03 00:42:43");
                ASSERT_EQUAL(parsed["repeat"], "FINISHED, +2 DAY");
                ASSERT_EQUAL(parsed["data"], "{\"something\":2}");
                ASSERT_EQUAL(parsed["priority"], "500");
                SASSERT(!parsed["created"].empty());
            }
        }
    }
} __QueryJobsTest;

