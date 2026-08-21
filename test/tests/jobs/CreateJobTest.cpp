#include <iostream>
#include <unistd.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct CreateJobTest : tpunit::TestFixture
{
    CreateJobTest()
        : tpunit::TestFixture("CreateJob",
                              BEFORE_CLASS(CreateJobTest::setupClass),
                              TEST(CreateJobTest::create),
                              TEST(CreateJobTest::createWithHttp),
                              TEST(CreateJobTest::createWithPriority),
                              TEST(CreateJobTest::createWithData),
                              TEST(CreateJobTest::createWithRepeat),
                              TEST(CreateJobTest::uniqueJob),
                              TEST(CreateJobTest::uniqueJobMergeData),
                              TEST(CreateJobTest::uniqueAsRetryLifecycle),
                              TEST(CreateJobTest::createWithBadData),
                              TEST(CreateJobTest::createWithBadRepeat),
                              TEST(CreateJobTest::createChildWithQueuedParent),
                              TEST(CreateJobTest::createChildWithRunningGrandparent),
                              TEST(CreateJobTest::retryRecurringJobs),
                              TEST(CreateJobTest::retryWithMalformedValue),
                              TEST(CreateJobTest::retryUnique),
                              TEST(CreateJobTest::retryLifecycle),
                              TEST(CreateJobTest::retryWithChildren),
                              TEST(CreateJobTest::getManualJobWithRetryAfter),
                              AFTER(CreateJobTest::tearDown),
                              AFTER_CLASS(CreateJobTest::tearDownClass))
    {
    }

    BedrockTester* tester;

    void setupClass()
    {
        tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});
    }

    // Reset the jobs table
    void tearDown()
    {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass()
    {
        delete tester;
    }

    void create()
    {
        SData command("CreateJob");
        string jobName = "testCreate";
        command["name"] = jobName;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(stol(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created but without the ms precision
        ASSERT_EQUAL(originalJob[0][4].substr(0, 19), originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(stol(originalJob[0][8]), 500);
        ASSERT_EQUAL(stol(originalJob[0][9]), 0);
    }

    void createWithHttp()
    {
        SData command("CreateJob / HTTP/1.1");
        string jobName = "testCreate";
        command["name"] = jobName;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(stol(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created but without the ms precision
        ASSERT_EQUAL(originalJob[0][4].substr(0, 19), originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(stol(originalJob[0][8]), 500);
        ASSERT_EQUAL(stol(originalJob[0][9]), 0);
    }

    void createWithPriority()
    {
        SData command("CreateJob");
        string jobName = "testCreate";
        string priority = "1000";
        command["name"] = jobName;
        command["priority"] = priority;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(stol(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created but without the ms precision
        ASSERT_EQUAL(originalJob[0][4].substr(0, 19), originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(originalJob[0][8], priority);
        ASSERT_EQUAL(stol(originalJob[0][9]), 0);
    }

    void createWithData()
    {
        SData command("CreateJob");
        string jobName = "testCreate";
        string data = "{\"blabla\":\"blabla\"}";
        command["name"] = jobName;
        command["data"] = data;
        const string& startTime = SCURRENT_TIMESTAMP();
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(stol(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun and created should be equal or higher to the time we started the test
        ASSERT_TRUE(originalJob[0][0].compare(startTime) >= 0);
        ASSERT_TRUE(originalJob[0][4].compare(startTime) >= 0);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], data);
        ASSERT_EQUAL(stol(originalJob[0][8]), 500);
        ASSERT_EQUAL(stol(originalJob[0][9]), 0);
    }

    void createWithRepeat()
    {
        SData command("CreateJob");
        string jobName = "testCreate";
        string repeat = "SCHEDULED, +1 HOUR";
        command["name"] = jobName;
        command["repeat"] = repeat;
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(stol(response["jobID"]), 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);
        ASSERT_EQUAL(originalJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(originalJob[0][1], response["jobID"]);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created but without the ms precision
        ASSERT_EQUAL(originalJob[0][4].substr(0, 19), originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], repeat);
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(stol(originalJob[0][8]), 500);
        ASSERT_EQUAL(stol(originalJob[0][9]), 0);
    }

    // Create a unique job
    // Then try to recreate the job with the same data
    // Make sure the new data is saved
    void uniqueJobMergeData()
    {
        // Create a unique job
        SData command("CreateJob");
        string jobName = "blabla";
        command["name"] = jobName;
        command["data"] = "{\"a\":1, \"b\":2, \"nestedObject\": {\"A\":1, \"B\":2}, \"nestedArray\":[1,2]}";
        command["unique"] = "true";
        STable response = tester->executeWaitVerifyContentTable(command);
        int64_t jobID = stol(response["jobID"]);
        ASSERT_GREATER_THAN(jobID, 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);

        // Try to recreate the job with the same data.
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(stol(response["jobID"]), jobID);

        // Try to recreate the job with new data, it should get updated.
        command["data"] = "{\"c\":3, \"d\":4, \"nestedObject\": {\"C\":3, \"D\":4}, \"nestedArray\":[3,4]}";
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(stol(response["jobID"]), jobID);

        SQResult updatedJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", updatedJob);
        ASSERT_EQUAL(updatedJob.size(), 1);
        // Assert the values are what we expect
        ASSERT_EQUAL(updatedJob[0][0], originalJob[0][0]);
        ASSERT_EQUAL(updatedJob[0][1], originalJob[0][1]);
        ASSERT_EQUAL(updatedJob[0][2], originalJob[0][2]);
        ASSERT_EQUAL(updatedJob[0][3], originalJob[0][3]);
        ASSERT_EQUAL(updatedJob[0][4], originalJob[0][4]);
        ASSERT_EQUAL(updatedJob[0][5], originalJob[0][5]);
        ASSERT_EQUAL(updatedJob[0][6], originalJob[0][6]);
        ASSERT_EQUAL(updatedJob[0][7], "{\"a\":1,\"b\":2,\"nestedObject\":{\"A\":1,\"B\":2,\"C\":3,\"D\":4},\"nestedArray\":[3,4],\"c\":3,\"d\":4}");
        ASSERT_EQUAL(updatedJob[0][8], originalJob[0][8]);
        ASSERT_EQUAL(updatedJob[0][9], originalJob[0][9]);
    }

    // Create a unique job
    // Then try to recreate the job with the some data
    // Make sure the new data is saved
    void uniqueJob()
    {
        // Create a unique job
        SData command("CreateJob");
        string jobName = "blabla";
        command["name"] = jobName;
        command["unique"] = "true";
        STable response = tester->executeWaitVerifyContentTable(command);
        int64_t jobID = stol(response["jobID"]);
        ASSERT_GREATER_THAN(jobID, 0);

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", originalJob);

        // Try to recreate the job with the same data.
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(stol(response["jobID"]), jobID);

        // Try to recreate the job with new data, it should get updated.
        string data = "{\"blabla\":\"test\"}";
        command["data"] = data;
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(stol(response["jobID"]), jobID);

        SQResult updatedJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", updatedJob);
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

        // Try to recreate the job with new data, without overwriting the existing data
        string data2 = "{\"blabla2\":\"test2\"}";
        command["data"] = data2;
        command["overwrite"] = "false";
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(stol(response["jobID"]), jobID);

        SQResult nonoverwritenJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID FROM jobs WHERE jobID = " + response["jobID"] + ";", nonoverwritenJob);
        ASSERT_EQUAL(updatedJob.size(), 1);
        // Assert that we have not overwritten the job data
        ASSERT_EQUAL(nonoverwritenJob[0][0], updatedJob[0][0]);
        ASSERT_EQUAL(nonoverwritenJob[0][1], updatedJob[0][1]);
        ASSERT_EQUAL(nonoverwritenJob[0][2], updatedJob[0][2]);
        ASSERT_EQUAL(nonoverwritenJob[0][3], updatedJob[0][3]);
        ASSERT_EQUAL(nonoverwritenJob[0][4], updatedJob[0][4]);
        ASSERT_EQUAL(nonoverwritenJob[0][5], updatedJob[0][5]);
        ASSERT_EQUAL(nonoverwritenJob[0][6], updatedJob[0][6]);
        ASSERT_EQUAL(nonoverwritenJob[0][7], updatedJob[0][7]);
        ASSERT_EQUAL(nonoverwritenJob[0][8], updatedJob[0][8]);
        ASSERT_EQUAL(nonoverwritenJob[0][9], updatedJob[0][9]);
    }

    void uniqueAsRetryLifecycle()
    {
        // The option accepts only Boolean values and requires a writable unique row.
        SData command("CreateJob");
        command["name"] = "invalidUniqueAsRetry";
        command["uniqueAsRetry"] = "sometimes";
        tester->executeWaitVerifyContent(command, "402 Malformed uniqueAsRetry");

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "disabledOverwrite";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        command["overwrite"] = "false";
        tester->executeWaitVerifyContent(command, "402 uniqueAsRetry requires unique=true and overwrite enabled");

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "missingUnique";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command, "402 uniqueAsRetry requires unique=true and overwrite enabled");

        // Callers cannot create Bedrock's private opt-in marker.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "reservedMetadata";
        command["data"] = "{\"activity\":1,\"_bedrockUniqueAsRetry\":{\"version\":999}}";
        const string reservedMetadataJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        SQResult result;
        tester->readDB("SELECT JSON_EXTRACT(data, '$._bedrockUniqueAsRetry'), JSON_EXTRACT(data, '$.activity') "
                       "FROM jobs WHERE jobID=" + reservedMetadataJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "");
        ASSERT_EQUAL(result[0][1], "1");

        // Equivalent JSON, including reordered nested object members, completes normally. Bedrock-owned fields do not
        // participate in the comparison, and worker output cannot overwrite the private marker.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "sameComparedData";
        command["data"] = "{\"activity\":1,\"nested\":{\"a\":1,\"b\":2}}";
        command["repeat"] = "FINISHED, +1 DAY";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string sameDataJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "QueryJob";
        command["jobID"] = sameDataJobID;
        const STable queriedJob = tester->executeWaitVerifyContentTable(command);
        ASSERT_TRUE(queriedJob.at("data").find("_bedrockUniqueAsRetry") == string::npos);
        ASSERT_TRUE(!SContains(queriedJob, "enqueueVersion"));

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "sameComparedData";
        STable runningJob = tester->executeWaitVerifyContentTable(command);
        ASSERT_TRUE(runningJob["data"].find("_bedrockUniqueAsRetry") == string::npos);
        ASSERT_TRUE(!SContains(runningJob, "enqueueVersion"));

        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = sameDataJobID;
        command["expectedData"] = "{\"nested\":{\"b\":2,\"a\":1},\"activity\":1,\"retryAfterCount\":99,"
            "\"originalNextRun\":\"1900-01-01 00:00:00\",\"_commitCounts\":{\"db\":999},"
            "\"_bedrockUniqueAsRetry\":false}";
        command["data"] = "{\"done\":true,\"_bedrockUniqueAsRetry\":false}";
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT state, JSON_TYPE(data, '$._bedrockUniqueAsRetry'), "
                       "JSON_EXTRACT(data, '$._bedrockUniqueAsRetry'), JSON_EXTRACT(data, '$.done') "
                       "FROM jobs WHERE jobID=" + sameDataJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "true");
        ASSERT_EQUAL(result[0][2], "1");
        ASSERT_EQUAL(result[0][3], "1");

        // A changed payload queues a subsequent run and discards stale worker output.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "finishComparedData";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string finishJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "finishComparedData";
        runningJob = tester->executeWaitVerifyContentTable(command);
        const string expectedFinishData = runningJob["data"];

        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishJobID;
        command["expectedData"] = "[]";
        tester->executeWaitVerifyContent(command, "402 expectedData is not a valid JSON Object");

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "finishComparedData";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishJobID;
        command["expectedData"] = expectedFinishData;
        command["data"] = "{\"activity\":1,\"worker\":true}";
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockUniqueAsRetry'), "
                       "JSON_EXTRACT(data, '$.activity'), JSON_EXTRACT(data, '$.worker') "
                       "FROM jobs WHERE jobID=" + finishJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "1");
        ASSERT_EQUAL(result[0][2], "2");
        ASSERT_EQUAL(result[0][3], "");

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "finishComparedData";
        runningJob = tester->executeWaitVerifyContentTable(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishJobID;
        command["expectedData"] = runningJob["data"];
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT COUNT(1) FROM jobs WHERE jobID=" + finishJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "0");

        // RetryJob preserves the newer enqueue's payload and attributes, but applies the worker's retry schedule.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "retryComparedData";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string retryJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "retryComparedData";
        runningJob = tester->executeWaitVerifyContentTable(command);
        const string expectedRetryData = runningJob["data"];

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "retryComparedData";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        command["jobPriority"] = "750";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = retryJobID;
        command["expectedData"] = expectedRetryData;
        command["nextRun"] = "2042-04-02 00:42:42";
        command["ignoreRepeat"] = "true";
        command["name"] = "staleName";
        command["jobPriority"] = "1000";
        command["data"] = "{\"activity\":1,\"worker\":true}";
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT state, name, nextRun, priority, JSON_EXTRACT(data, '$.activity'), "
                       "JSON_EXTRACT(data, '$.worker') "
                       "FROM jobs WHERE jobID=" + retryJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "retryComparedData");
        ASSERT_EQUAL(result[0][2], "2042-04-02 00:42:42");
        ASSERT_EQUAL(result[0][3], "750");
        ASSERT_EQUAL(result[0][4], "2");
        ASSERT_EQUAL(result[0][5], "");

        // A job can opt in while it is already running. The immutable snapshot still detects the newer payload.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "legacyWorker";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        const string legacyJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "legacyWorker";
        STable legacyJob = tester->executeWaitVerifyContentTable(command);

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "legacyWorker";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = legacyJobID;
        command["expectedData"] = legacyJob["data"];
        tester->executeWaitVerifyContent(command);

        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockUniqueAsRetry'), JSON_EXTRACT(data, '$.activity') "
                       "FROM jobs WHERE jobID=" + legacyJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "1");
        ASSERT_EQUAL(result[0][2], "2");

        // Missing expectedData keeps legacy completion behavior during a rolling BWM deployment.
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "legacyCompletion";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string legacyCompletionJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "legacyCompletion";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "legacyCompletion";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = legacyCompletionJobID;
        tester->executeWaitVerifyContent(command);
        tester->readDB("SELECT COUNT(1) FROM jobs WHERE jobID=" + legacyCompletionJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "0");
    }

    void createWithBadData()
    {
        SData command("CreateJob");
        command["name"] = "blabla";
        command["data"] = "blabla";
        tester->executeWaitVerifyContent(command, "402 Data is not a valid JSON Object");
    }

    void createWithBadRepeat()
    {
        SData command("CreateJob");
        command["name"] = "blabla";
        command["repeat"] = "blabla";
        tester->executeWaitVerifyContent(command, "402 Malformed repeat");
    }

    // Cannot create a child job when parent is QUEUED
    void createChildWithQueuedParent()
    {
        // Create a parent job
        SData command("CreateJob");
        command["name"] = "parent";

        STable response = tester->executeWaitVerifyContentTable(command);
        string parentID = response["jobID"];

        // Try to create the child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child";
        command["parentJobID"] = parentID;
        tester->executeWaitVerifyContent(command, "405 Can only create child job when parent is RUNNING, RUNQUEUED or PAUSED");
    }

    // Cannot create a job with a running grandparent
    void createChildWithRunningGrandparent()
    {
        // Create a parent job
        SData command("CreateJob");
        command["name"] = "parent";
        STable response = tester->executeWaitVerifyContentTable(command);
        string parentID = response["jobID"];

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWaitVerifyContent(command);

        // Create the child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string childID = response["jobID"];

        // Assert parent is still running
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Try to create grandchild
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "grandchild";
        command["parentJobID"] = childID;
        tester->executeWaitVerifyContent(command, "405 Cannot create grandchildren");
    }

    void retryRecurringJobs()
    {
        // Create a job with both retry and repeat
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+5 SECOND";
        string repeatValue = "SCHEDULED, +10 SECONDS";
        command["name"] = jobName;
        command["repeat"] = repeatValue;
        command["retryAfter"] = retryValue;

        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID, retryAfter FROM jobs WHERE jobID = " + jobID + ";", originalJob);

        ASSERT_EQUAL(originalJob[0][1], jobID);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        ASSERT_EQUAL(originalJob[0][3], jobName);
        // nextRun should equal created but without the ms precision
        ASSERT_EQUAL(originalJob[0][4].substr(0, 19), originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], repeatValue);
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(stol(originalJob[0][8]), 500);
        ASSERT_EQUAL(stol(originalJob[0][9]), 0);
        ASSERT_EQUAL(originalJob[0][10], retryValue);

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = jobName;
        response = tester->executeWaitVerifyContentTable(command);

        ASSERT_EQUAL(response["data"], "{}");
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);

        // Query the db and confirm the state, and that nextRun and lastRun are 5 seconds apart because of retryAfter
        // Confirm the data contains an updated retryAfterCount since it was fetched once.
        SQResult jobData;
        tester->readDB("SELECT state, nextRun, lastRun, data FROM jobs WHERE jobID = " + jobID + ";", jobData);
        ASSERT_EQUAL(jobData[0][0], "RUNQUEUED");
        time_t nextRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][1]);
        time_t lastRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][2]);
        ASSERT_EQUAL(difftime(nextRunTime, lastRunTime), 5);
        ASSERT_EQUAL(jobData[0][3], "{\"retryAfterCount\":1,\"originalNextRun\":\"" + originalJob[0][4] + "\"}");

        // Get the job, confirm error because 1 second hasn't passed
        try {
            tester->executeWaitVerifyContent(command, "404 No job found");
        } catch (...) {
            cout << "retryRecurringJobs failed at point 1." << endl;
            throw;
        }

        // Try and get it repeatedly. Should fail a couple times and then succeed.
        int retries = 9;
        bool success = false;
        while (retries-- > 0) {
            try {
                // Let it repeat until it works or we run out of retries.
                response = tester->executeWaitVerifyContentTable(command);
                ASSERT_EQUAL(response["data"], "{\"retryAfterCount\":1,\"originalNextRun\":\"" + originalJob[0][4] + "\"}");
                ASSERT_EQUAL(response["jobID"], jobID);
                ASSERT_EQUAL(response["name"], jobName);
            } catch (...) {
                sleep(1);
                continue;
            }

            // Now it should fail again.
            while (retries-- > 0) {
                try {
                    tester->executeWaitVerifyContent(command, "404 No job found");
                    success = true;
                    break;
                } catch (...) {
                    sleep(1);
                    continue;
                }
            }
        }
        ASSERT_TRUE(success);

        // Finish the job
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Query db and confirm job still exists
        tester->readDB("SELECT state, nextRun, lastRun FROM jobs WHERE jobID = " + jobID + ";", jobData);
        nextRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][1]);
        lastRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][2]);
        ASSERT_EQUAL(jobData[0][0], "QUEUED");
        // Test for accurate delta between lastRun and nextRun (should be 10s) with a 1s accuracy margin
        ASSERT_TRUE(9 <= difftime(nextRunTime, lastRunTime) && difftime(nextRunTime, lastRunTime) <= 11);
    }

    void retryWithMalformedValue()
    {
        SData command("CreateJob");
        command["name"] = "test";
        command["retryAfter"] = "10";
        tester->executeWaitVerifyContent(command, "402 Malformed retryAfter");
    }

    void retryUnique()
    {
        SData command("CreateJob");
        command["name"] = "test";
        command["retryAfter"] = "+10 HOUR";
        command["unique"] = "true";
        tester->executeWaitVerifyContent(command, "200 OK");
    }

    void retryLifecycle()
    {
        // Create a retryable job
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+5 SECONDS";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;

        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Query the db to confirm it was created correctly
        SQResult originalJob;
        tester->readDB("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, priority, parentJobID, retryAfter FROM jobs WHERE jobID = " + jobID + ";", originalJob);
        ASSERT_EQUAL(originalJob[0][1], jobID);
        ASSERT_EQUAL(originalJob[0][2], "QUEUED");
        // nextRun should equal created but without the ms precision
        ASSERT_EQUAL(originalJob[0][4].substr(0, 19), originalJob[0][0]);
        ASSERT_EQUAL(originalJob[0][5], "");
        ASSERT_EQUAL(originalJob[0][6], "");
        ASSERT_EQUAL(originalJob[0][7], "{}");
        ASSERT_EQUAL(stol(originalJob[0][8]), 500);
        ASSERT_EQUAL(stol(originalJob[0][9]), 0);
        ASSERT_EQUAL(originalJob[0][10], retryValue);

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = jobName;
        response = tester->executeWaitVerifyContentTable(command);

        ASSERT_EQUAL(response["data"], "{}");
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);

        // Query the db and confirm that state, nextRun and lastRun are 5 seconds apart
        SQResult jobData;
        tester->readDB("SELECT state, nextRun, lastRun FROM jobs WHERE jobID = " + jobID + ";", jobData);
        ASSERT_EQUAL(jobData[0][0], "RUNQUEUED");
        time_t nextRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][1]);
        time_t lastRunTime = JobTestHelper::getTimestampForDateTimeString(jobData[0][2]);
        ASSERT_EQUAL(difftime(nextRunTime, lastRunTime), 5);

        // Get the job, confirm error
        try {
            // This needs to run less than 5 seconds after the first `GetJob` or it doesn't work.
            tester->executeWaitVerifyContent(command, "404 No job found");
        } catch (...) {
            cout << "CreateJobTest failed at point 1." << endl;
            throw;
        }

        // This will fail with 404's until the job re-queues.
        uint64_t start = STimeNow();
        bool assertionsChecked = false;
        while (STimeNow() < start + 10'000'000) {
            try {
                response = tester->executeWaitVerifyContentTable(command);
            } catch (...) {
                usleep(100'000);
                continue;
            }
            ASSERT_EQUAL(response["data"], "{\"retryAfterCount\":1}");
            ASSERT_EQUAL(response["jobID"], jobID);
            ASSERT_EQUAL(response["name"], jobName);
            assertionsChecked = true;
            break;
        }
        ASSERT_TRUE(assertionsChecked);

        // try again immediately and it should be not found.
        try {
            tester->executeWaitVerifyContent(command, "404 No job found");
        } catch (...) {
            cout << "CreateJobTest failed at point 2." << endl;
            throw;
        }

        // Finish the job
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Query db and confirm job doesn't exist
        tester->readDB("SELECT state, nextRun, lastRun, FROM jobs WHERE jobID = " + jobID + ";", jobData);
        ASSERT_TRUE(jobData.empty());
    }

    void retryWithChildren()
    {
        SData command("CreateJob");
        string jobName = "testRetryable";
        string retryValue = "+5 SECONDS";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;

        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Try to create child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testRetryableChild";
        command["parentJobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can only create child job when parent is RUNNING, RUNQUEUED or PAUSED");
    }

    void getManualJobWithRetryAfter()
    {
        // Create a job with both retry and repeat
        SData command("CreateJob");
        string jobName = "manual/testRetryable";
        string retryValue = "+5 SECOND";
        command["name"] = jobName;
        command["retryAfter"] = retryValue;
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = jobName;
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response["data"], "{}");
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);

        // Check the jobs retryAfter, confirm it does have a retryAfter value
        SQResult jobData;
        tester->readDB("SELECT data FROM jobs WHERE jobID = " + jobID + ";", jobData);
        ASSERT_EQUAL(jobData[0][0], "{}");

        // Finish the job
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);
    }
} __CreateJobTest;
