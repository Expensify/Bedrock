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
        // Create a unique job
        SData command("CreateJob");
        string jobName = "blabla";
        command["name"] = jobName;
        command["unique"] = "true";
        STable response = getJsonResult(command);
        int jobID = SToInt(response["jobID"]);
        ASSERT_GREATER_THAN(jobID, 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID, retryAfter FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);

        // Try to recreate the job with new data
        string data = "{\"blabla\":\"test\"}";
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = jobName;
        command["unique"] = "true";
        command["data"] = data;
        response = getJsonResult(command);
        ASSERT_EQUAL(SToInt(response["jobID"]), jobID);

        SQResult updatedJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID, retryAfter FROM jobs WHERE jobID = " + response["jobID"] + ";", updatedJob);
        ASSERT_EQUAL(updatedJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(updatedJob[0][0], originalJob[0][0]);
        ASSERT_EQUAL(updatedJob[0][1], originalJob[0][1]);
        ASSERT_EQUAL(updatedJob[0][2], originalJob[0][2]);
        ASSERT_EQUAL(updatedJob[0][3], originalJob[0][3]);
        ASSERT_EQUAL(updatedJob[0][4], originalJob[0][4]);
        ASSERT_EQUAL(updatedJob[0][5], originalJob[0][5]);
        ASSERT_EQUAL(updatedJob[0][6], originalJob[0][6]);
        ASSERT_EQUAL(updatedJob[0][7], data);
        ASSERT_EQUAL(updatedJob[0][8], originalJob[0][8]);
        ASSERT_EQUAL(updatedJob[0][9], originalJob[0][9]);
    }

    void badData() {
        SData command("CreateJob");
        command["data"] = "blabla";
        tester->executeWait(command, "402 Data is not a valid JSON Object");
    }

    STable getJsonResult(SData command) {
        string resultJson = tester->executeWait(command);
        return SParseJSONObject(resultJson);
    }
} __CreateJobTest;


