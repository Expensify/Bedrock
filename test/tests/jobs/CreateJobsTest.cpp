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
        command["jobs"] = "[{\"name\":\"testCreate\", \"data\":{\"blabla\":\"blabla\"}, \"repeat\": \"SCHEDULED, +1 HOUR\"}, {\"name\":\"testCreate2\", \"data\":{\"nope\":\"nope\"}}]";
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobIDList = SParseJSONArray(response["jobIDs"]);
        ASSERT_EQUAL(jobIDList.size(), 2);
        string jobID1 = jobIDList.front();
        string jobID2 = jobIDList.back();
        ASSERT_NOT_EQUAL(jobID1, jobID2);
    }

    void createWithHttp() {
        SData command("CreateJobs / HTTP/1.1");
        command["jobs"] = "[{\"name\":\"testCreate\", \"data\":{\"blabla\":\"blabla\"}, \"repeat\": \"SCHEDULED, +1 HOUR\"}, {\"name\":\"testCreate2\", \"data\":{\"nope\":\"nope\"}}]";
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobIDList = SParseJSONArray(response["jobIDs"]);
        ASSERT_EQUAL(jobIDList.size(), 2);
        string jobID1 = jobIDList.front();
        string jobID2 = jobIDList.back();
        ASSERT_NOT_EQUAL(jobID1, jobID2);
    }

    void createWithInvalidJson() {
        SData command("CreateJobs");
        command["jobs"] = "[{\"name\":\"testCreate\", \"data\":{\"blabla\":\"blabla\"}, \"parentJobID\":\"1000\", \"repeat\": \"SCHEDULED, +1 HOUR\"}, {\"testCreate2\", \"data\":{\"nope\":\"nope\"}}]";
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
        command["jobs"] = "[{\"name\":\"testCreate\", \"data\":{\"blabla\":\"blabla\"}, \"parentJobID\":\"" + response["jobID"] +"\", \"repeat\": \"SCHEDULED, +1 HOUR\"}, {\"name\":\"testCreate2\", \"data\":{\"nope\":\"nope\"}}]";
        tester->executeWaitVerifyContent(command, "405 Can only create child job when parent is RUNNING or PAUSED");
    }
} __CreateJobsTest;
