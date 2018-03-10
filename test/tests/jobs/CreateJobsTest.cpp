#include <test/lib/BedrockTester.h>

struct CreateJobsTest : tpunit::TestFixture {
    CreateJobsTest()
        : tpunit::TestFixture("CreateJobs",
                              BEFORE_CLASS(CreateJobsTest::setupClass),
                              TEST(CreateJobsTest::create),
                              TEST(CreateJobsTest::createWithHttp),
                              TEST(CreateJobsTest::createWithInvalidJson),
                              TEST(CreateJobsTest::createWithParentIDNotRunning),
                              AFTER(CreateJobsTest::tearDown),
                              AFTER_CLASS(CreateJobsTest::tearDownClass)) { }

    BedrockTester* tester;

    string _generateCreateJobContentJSON() {
        STable job1Content;
        STable data1;
        data1["blabla"] = "blabla";
        job1Content["name"] = "testCreate";
        job1Content["data"] = SComposeJSONObject(data1);
        job1Content["repeat"] = "SCHEDULED, +1 HOUR";

        STable job2Content;
        STable data2;
        data2["nope"] = "nope";
        job2Content["name"] = "testCreate2";
        job2Content["data"] = SComposeJSONObject(data2);

        vector<string> jobs;
        jobs.push_back(SComposeJSONObject(job1Content));
        jobs.push_back(SComposeJSONObject(job2Content));

        return SComposeJSONArray(jobs);
    }

    void setupClass() { tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    void create() {
        SData command("CreateJobs");
        command["jobs"] = _generateCreateJobContentJSON();
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobIDList = SParseJSONArray(response["jobIDs"]);
        ASSERT_EQUAL(jobIDList.size(), 2);
        string jobID1 = jobIDList.front();
        string jobID2 = jobIDList.back();
        ASSERT_NOT_EQUAL(jobID1, jobID2);
    }

    void createWithHttp() {
        SData command("CreateJobs / HTTP/1.1");
        command["jobs"] = _generateCreateJobContentJSON();
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobIDList = SParseJSONArray(response["jobIDs"]);
        ASSERT_EQUAL(jobIDList.size(), 2);
        string jobID1 = jobIDList.front();
        string jobID2 = jobIDList.back();
        ASSERT_NOT_EQUAL(jobID1, jobID2);
    }

    void createWithInvalidJson() {
        SData command("CreateJobs");
        command["jobs"] = _generateCreateJobContentJSON() + "}";
        tester->executeWaitVerifyContent(command, "401 Invalid JSON");
    }

    void createWithParentIDNotRunning() {
        // First create parent job
        SData command("CreateJob");
        command["name"] = "blabla";
        STable response = tester->executeWaitVerifyContentTable(command);

        // Now try to create two new jobs
        command.clear();
        command.methodLine = "CreateJobs";

        STable job1Content;
        STable data1;
        data1["blabla"] = "blabla";
        job1Content["name"] = "testCreate";
        job1Content["data"] = SComposeJSONObject(data1);
        job1Content["repeat"] = "SCHEDULED, +1 HOUR";
        job1Content["parentJobID"] = response["jobID"];

        STable job2Content;
        STable data2;
        data2["nope"] = "nope";
        job2Content["name"] = "testCreate2";
        job2Content["data"] = SComposeJSONObject(data2);

        vector<string> jobs;
        jobs.push_back(SComposeJSONObject(job1Content));
        jobs.push_back(SComposeJSONObject(job2Content));
        command["jobs"] = SComposeJSONArray(jobs);
        tester->executeWaitVerifyContent(command, "405 Can only create child job when parent is RUNNING or PAUSED");
    }
} __CreateJobsTest;
