#include <test/lib/BedrockTester.h>

struct CreateJobTest : tpunit::TestFixture {
    CreateJobTest()
        : tpunit::TestFixture("CreateJob",
                              BEFORE_CLASS(CreateJobTest::setup),
                              TEST(CreateJobTest::create),
                              TEST(CreateJobTest::createWithPriority),
                              TEST(CreateJobTest::createWithData),
                              TEST(CreateJobTest::createWithRepeat),
                              TEST(CreateJobTest::uniqueJob),
                              TEST(CreateJobTest::createWithBadData),
                              TEST(CreateJobTest::createWithBadRepeat),
                              AFTER_CLASS(CreateJobTest::tearDown)) { }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    void create() {
        SData command("CreateJob");
        string jobName = "testCreate";
        command["name"] = jobName;
        STable response = getJsonResult(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

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
    }

    void createWithPriority() {
        SData command("CreateJob");
        string jobName = "testCreate";
        string priority = "1000";
        command["name"] = jobName;
        command["priority"] = priority;
        STable response = getJsonResult(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

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
        ASSERT_EQUAL(originalJob[0][8], priority);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    void createWithData() {
        SData command("CreateJob");
        string jobName = "testCreate";
        string data = "{\"blabla\":\"blabla\"}";
        command["name"] = jobName;
        command["data"] = data;
        STable response = getJsonResult(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

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
        ASSERT_EQUAL(originalJob[0][7], data);
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    void createWithRepeat() {
        SData command("CreateJob");
        string jobName = "testCreate";
        string repeat = "SCHEDULED, +1 HOUR";
        command["name"] = jobName;
        command["repeat"] = repeat;
        STable response = getJsonResult(command);
        ASSERT_GREATER_THAN(SToInt(response["jobID"]), 0);

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
        ASSERT_EQUAL(originalJob[0][6], repeat);
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
    }

    // Create a unique job
    // Then try to recreate the job with the some data
    // Make sure the new data is saved
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

    void createWithBadData() {
        SData command("CreateJob");
        command["name"] = "blabla";
        command["data"] = "blabla";
        tester->executeWait(command, "402 Data is not a valid JSON Object");
    }

    void createWithBadRepeat() {
        SData command("CreateJob");
        command["name"] = "blabla";
        command["repeat"] = "blabla";
        tester->executeWait(command, "402 Malformed repeat");
    }

    STable getJsonResult(SData command) {
        string resultJson = tester->executeWait(command);
        return SParseJSONObject(resultJson);
    }
} __CreateJobTest;


