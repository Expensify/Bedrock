#include <test/lib/BedrockTester.h>

struct CreateJobTest : tpunit::TestFixture {
    CreateJobTest()
        : tpunit::TestFixture("CreateJob",
                              BEFORE_CLASS(CreateJobTest::setup),
                              TEST(CreateJobTest::uniqueJob),
                              AFTER_CLASS(CreateJobTest::tearDown)) { }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    void uniqueJob() {
        SData command("CreateJob");
        string jobName = "blabla";
        command["name"] = jobName;
        STable response = getJsonResult(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

        SQResult result;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID, retryAfter FROM jobs WHERE jobID = " + response["jobID"] + ";", result);
        ASSERT_EQUAL(result.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(result[0][1], response["jobID"]);
        ASSERT_EQUAL(result[0][2], "QUEUED");
        ASSERT_EQUAL(result[0][3], jobName);
        // nextRun should equal created
        ASSERT_EQUAL(result[0][4], result[0][0]);
        ASSERT_EQUAL(result[0][5], "");
        ASSERT_EQUAL(result[0][6], "");
        ASSERT_EQUAL(result[0][7], "{}");
        ASSERT_EQUAL(SToInt(result[0][8]), 500);
        ASSERT_EQUAL(SToInt(result[0][9]), 0);
    }

    STable getJsonResult(SData command) {
        string resultJson = tester->executeWait(command);
        return SParseJSONObject(resultJson);
    }
} __CreateJobTest;


