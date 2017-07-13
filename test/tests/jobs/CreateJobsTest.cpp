#include <test/lib/BedrockTester.h>

struct CreateJobsTest : tpunit::TestFixture {
    CreateJobsTest()
        : tpunit::TestFixture("CreateJobs",
                              BEFORE_CLASS(CreateJobsTest::setup),
                              TEST(CreateJobsTest::create),
                              TEST(CreateJobsTest::createWithInvalidJson),
                              TEST(CreateJobsTest::createWithParentIDNotRunning),
                              AFTER_CLASS(CreateJobsTest::tearDown)) { }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    void create() {
        SData command("CreateJobs");
        command["jobs"] = "[{\"name\":\"testCreate\", \"data\":{\"blabla\":\"blabla\"}, \"repeat\": \"SCHEDULED, +1 HOUR\"}, {\"name\":\"testCreate2\", \"data\":{\"nope\":\"nope\"}}]";
        STable response = getJsonResult(command);
        list<string> jobIDList = SParseJSONArray(response["jobIDs"]);
        ASSERT_EQUAL(jobIDList.size(), 2);
        string jobID1 = jobIDList.front();
        string jobID2 = jobIDList.back();
        ASSERT_NOT_EQUAL(jobID1, jobID2);
    }

    void createWithInvalidJson() {
        SData command("CreateJobs");
        command["jobs"] = "[{\"name\":\"testCreate\", \"data\":{\"blabla\":\"blabla\"}, \"parentJobID\":\"1000\", \"repeat\": \"SCHEDULED, +1 HOUR\"}, {\"testCreate2\", \"data\":{\"nope\":\"nope\"}}]";
        tester->executeWait(command, "401 Invalid JSON");
    }

    void createWithParentIDNotRunning() {
        // First create parent job
        SData command("CreateJob");
        command["name"] = "blabla";
        STable response = getJsonResult(command);

        // Now try to create two new jobs
        command.clear();
        command.methodLine = "CreateJobs";
        command["jobs"] = "[{\"name\":\"testCreate\", \"data\":{\"blabla\":\"blabla\"}, \"parentJobID\":\"" + response["jobID"] +"\", \"repeat\": \"SCHEDULED, +1 HOUR\"}, {\"name\":\"testCreate2\", \"data\":{\"nope\":\"nope\"}}]";
        tester->executeWait(command, "405 Can only create child job when parent is RUNNING or PAUSED");
    }

    STable getJsonResult(SData command) {
        string resultJson = tester->executeWait(command);
        return SParseJSONObject(resultJson);
    }
} __CreateJobsTest;
