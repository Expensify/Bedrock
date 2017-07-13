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
                              TEST(CreateJobTest::retryRecurringJobs),
                              TEST(CreateJobTest::retryWithMalformedValue),
                              TEST(CreateJobTest::retryUnique),
                              TEST(CreateJobTest::retryLifecycle),
                              TEST(CreateJobTest::retryWithChildren),
                              //TEST(CreateJobTest::retryJobComesFirst),
                              AFTER_CLASS(CreateJobTest::tearDown)) { }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    // TODO: delete this, it's only for debugging purposes
    void gettime() {
        time_t rawtime;
        struct tm * timeinfo;
        char buffer[80];

        time (&rawtime);
        timeinfo = localtime(&rawtime);

        strftime(buffer,sizeof(buffer),"%d-%m-%Y %I:%M:%S",timeinfo);
        string str(buffer);

        cout << "current time " << str<<endl;
    }

    // TODO: delete this, it's only for debugging purposes
    void dumptable() {
        SQResult temp;
        tester->readDB("SELECT * FROM jobs;", temp);
        cout << temp.serializeToText() << endl;
    }

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

    void retryRecurringJobs() {
        SData command("CreateJob");
        command["name"] = "test";
        command["repeat"] = "SCHEDULED, +1 HOUR";
        command["retryAfter"] = "10";
        tester->executeWait(command, "402 Recurring auto-retrying jobs are not supported");
    }

    void retryWithMalformedValue() {
        SData command("CreateJob");
        command["name"] = "test";
        command["retryAfter"] = "10";
        tester->executeWait(command, "402 Malformed retryAfter");
    }

    void retryUnique() {
        SData command("CreateJob");
        command["name"] = "test";
        command["retryAfter"] = "+10 HOUR";
        command["unique"] = "true";
        tester->executeWait(command, "405 Unique jobs can't be retried");
    }

    void retryLifecycle() {
        // Create a retryable job
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+5 SECONDS";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;

        STable response = getJsonResult(command);
        string jobID = response["jobID"];

        // Query the db to confirm it was created correctly
        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID, retryAfter FROM jobs WHERE jobID = " + jobID + ";", originalJob);
        ASSERT_EQUAL(originalJob[0][1], jobID);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        ASSERT_EQUAL(originalJob[0][4], originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(SToInt(originalJob[0][8]), 500);
        ASSERT_EQUAL(SToInt(originalJob[0][9]), 0);
        ASSERT_EQUAL(originalJob[0][10], retryValue);

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = jobName;
        response = getJsonResult(command);

        ASSERT_EQUAL(response["data"], "{}");
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);

        // Query the db and confirm that state, nextRun and lastRun are correct
        SQResult jobData;
        tester->readDB("SELECT state, nextRun, lastRun, FROM jobs WHERE jobID = " + jobID + ";", jobData);

        ASSERT_EQUAL(jobData[0][0], "RUNQUEUED");
        string nextRun = jobData[0][1];
        string lastRun = jobData[0][2];
        struct tm tm;
        strptime(nextRun.c_str(), "%Y-%m-%d %H:%i:%s", &tm);
        time_t nextRunTime = mktime(&tm);
        strptime(lastRun.c_str(), "%Y-%m-%d %H:%i:%s", &tm);
        time_t lastRunTime = mktime(&tm);
        ASSERT_EQUAL(difftime(nextRunTime, lastRunTime), 5);

        // Get the job, confirm error
        tester->executeWait(command, "404 No job found");
        // Wait 5 seconds, get the job, confirm no error
        sleep(5);
        response = getJsonResult(command);

        ASSERT_EQUAL(response["data"], "{}");
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);

        // Get the job, confirm error
        tester->executeWait(command, "404 No job found");

        // Try to update job, get error because of no data
        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = jobID;
        tester->executeWait(command, "402 Missing data");

        // Try to update job, get error because it's running
        command["data"] = "{}";
        tester->executeWait(command, "402 Auto-retrying jobs cannot be updated once running");

        // Finish the job
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWait(command);

        // Query db and confirm job doesn't exist
        tester->readDB("SELECT state, nextRun, lastRun, FROM jobs WHERE jobID = " + jobID + ";", jobData);
        ASSERT_TRUE(jobData.empty());
    }

    void retryWithChildren() {
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+5 SECONDS";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;

        STable response = getJsonResult(command);
        string jobID = response["jobID"];

        // Try to create child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testRetryableChild";
        command["parentJobID"] = jobID;
        tester->executeWait(command, "402 Auto-retrying parents cannot have children");
    }

    // This should actually go in a GetJob test
    // Retryable job (after retry period has passed) comes before QUEUED job
    void retryJobComesFirst() {
        // Create two retryable jobs
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+2 SECONDS";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;
        STable response = getJsonResult(command);
        string retryableJob1 = response["jobID"];

        response = getJsonResult(command);
        string retryableJob2 = response["jobID"];

        // Get a job and confirm it's the first job we created
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "*Retryable";
        response = getJsonResult(command);
        ASSERT_EQUAL(response["jobID"], retryableJob1);

        // Create a non-retryable job
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "notRetryable";

        response = getJsonResult(command);
        string nonRetryableJob = response["jobID"];

        // Sleep 9 seconds
        sleep(9);

        dumptable();

        // Get a job, should be the first job we created (because it's been in REQUEUED for more than the retryAfter time)
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "*Retryable";
        response = getJsonResult(command);
        cout << "job 1 " << retryableJob1 << endl;
        cout << "job 2 " << retryableJob2 << endl;
        cout << "this job " << response["jobID"] << endl;
        gettime();
        ASSERT_EQUAL(response["jobID"], retryableJob1);
    }

    // TODO: put this in a util file
    STable getJsonResult(SData command) {
        string resultJson = tester->executeWait(command);
        return SParseJSONObject(resultJson);
    }
} __CreateJobTest;
