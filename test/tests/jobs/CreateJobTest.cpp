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
                              TEST(CreateJobTest::uniqueAsRetryStrictDataAndSnapshot),
                              TEST(CreateJobTest::uniqueAsRetryCannotOwnChildren),
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
        // Given a non-Boolean opt-in value that cannot define a stable retry policy
        SData command("CreateJob");
        command["name"] = "invalidRerunIfDataChanged";
        command["uniqueAsRetry"] = "sometimes";

        // When the caller requests rerun-if-data-changed behavior
        // Then Bedrock rejects the request because the policy must be unambiguous
        tester->executeWaitVerifyContent(command, "402 Malformed uniqueAsRetry");

        // Given a unique job that prevents updates to its existing row
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "disabledOverwrite";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        command["overwrite"] = "false";

        // When the caller requests rerun-if-data-changed behavior
        // Then Bedrock rejects the request because duplicate activity must update the existing row
        tester->executeWaitVerifyContent(command, "402 uniqueAsRetry requires unique=true and overwrite enabled");

        // Given a non-unique job that cannot provide a single execution lane
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "missingUnique";
        command["uniqueAsRetry"] = "true";

        // When the caller requests rerun-if-data-changed behavior
        // Then Bedrock rejects the request because concurrent rows violate the retry contract
        tester->executeWaitVerifyContent(command, "402 uniqueAsRetry requires unique=true and overwrite enabled");

        // Given caller data that tries to set Bedrock's private opt-in marker
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "reservedMetadata";
        command["data"] = "{\"activity\":1,\"_bedrockRerunIfDataChanged\":true}";

        // When Bedrock creates the job
        const string reservedMetadataJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        // Then Bedrock removes the marker because callers must not control infrastructure state
        SQResult result;
        tester->readDB("SELECT JSON_EXTRACT(data, '$._bedrockRerunIfDataChanged'), JSON_EXTRACT(data, '$.activity') "
                       "FROM jobs WHERE jobID=" + reservedMetadataJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "");
        ASSERT_EQUAL(result[0][1], "1");

        // Given an existing unique job with caller-selected scheduling fields
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "markerOnlyOptIn";
        command["data"] = "{\"activity\":1}";
        command["repeat"] = "FINISHED, +1 DAY";
        command["jobPriority"] = "750";
        command["unique"] = "true";
        const string markerOnlyJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        // When an identical enqueue enables rerun-if-data-changed without new scheduling fields
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "markerOnlyOptIn";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        ASSERT_EQUAL(tester->executeWaitVerifyContentTable(command)["jobID"], markerOnlyJobID);

        // Then Bedrock adds the marker without changing the schedule
        tester->readDB("SELECT repeat, priority, JSON_TYPE(data, '$._bedrockRerunIfDataChanged'), "
                       "JSON_EXTRACT(data, '$._bedrockRerunIfDataChanged') "
                       "FROM jobs WHERE jobID=" + markerOnlyJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "FINISHED, +1 DAY");
        ASSERT_EQUAL(result[0][1], "750");
        ASSERT_EQUAL(result[0][2], "true");
        ASSERT_EQUAL(result[0][3], "1");

        // Given an opted-in recurring job with nested caller data
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "sameComparedData";
        command["data"] = "{\"activity\":1,\"nested\":{\"a\":1,\"b\":2}}";
        command["repeat"] = "FINISHED, +1 DAY";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string sameDataJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        // When public APIs inspect and dequeue the job
        command.clear();
        command.methodLine = "QueryJob";
        command["jobID"] = sameDataJobID;
        const STable queriedJob = tester->executeWaitVerifyContentTable(command);

        // Then responses hide the private marker
        ASSERT_TRUE(queriedJob.at("data").find("_bedrockRerunIfDataChanged") == string::npos);

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "sameComparedData";
        STable runningJob = tester->executeWaitVerifyContentTable(command);
        ASSERT_TRUE(runningJob["data"].find("_bedrockRerunIfDataChanged") == string::npos);

        // Given duplicate enqueues that change and then restore the caller data
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "sameComparedData";
        command["data"] = "{\"activity\":2}";
        command["repeat"] = "FINISHED, +1 DAY";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        command["data"] = "{\"activity\":1}";
        tester->executeWaitVerifyContent(command);

        // When the worker finishes with reordered data and unrelated infrastructure fields
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = sameDataJobID;
        command["expectedData"] = "{\"nested\":{\"b\":2,\"a\":1},\"activity\":1,\"retryAfterCount\":99,"
            "\"originalNextRun\":\"1900-01-01 00:00:00\",\"_commitCounts\":{\"db\":999},"
            "\"_bedrockRerunIfDataChanged\":false}";
        command["data"] = "{\"done\":true,\"_bedrockRerunIfDataChanged\":false}";
        tester->executeWaitVerifyContent(command);

        // Then Bedrock completes the attempt because semantic caller data matches the immutable snapshot
        tester->readDB("SELECT state, JSON_TYPE(data, '$._bedrockRerunIfDataChanged'), "
                       "JSON_EXTRACT(data, '$._bedrockRerunIfDataChanged'), JSON_EXTRACT(data, '$.done') "
                       "FROM jobs WHERE jobID=" + sameDataJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "true");
        ASSERT_EQUAL(result[0][2], "1");
        ASSERT_EQUAL(result[0][3], "1");

        // Given an opted-in worker that reports progress without a duplicate enqueue
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "progressOnlyUpdate";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string progressOnlyJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "progressOnlyUpdate";
        runningJob = tester->executeWaitVerifyContentTable(command);

        command.clear();
        command.methodLine = "UpdateJob";
        command["jobID"] = progressOnlyJobID;
        command["data"] = "{\"activity\":1,\"progress\":50}";
        tester->executeWaitVerifyContent(command);

        // When the worker finishes with its immutable dequeue snapshot
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = progressOnlyJobID;
        command["expectedData"] = SDecodeBase64(runningJob.at("expectedDataBase64"));
        tester->executeWaitVerifyContent(command);

        // Then Bedrock requeues the job because its current data no longer matches the immutable snapshot
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockRerunIfDataChanged'), "
                       "JSON_EXTRACT(data, '$.progress') FROM jobs WHERE jobID=" + progressOnlyJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "1");
        ASSERT_EQUAL(result[0][2], "50");

        // Given an opted-in worker with an immutable snapshot of the original activity
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
        const string expectedFinishData = SDecodeBase64(runningJob.at("expectedDataBase64"));

        // When the worker sends malformed expected data
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishJobID;
        command["expectedData"] = "[]";

        // Then Bedrock rejects the terminal request because freshness comparison requires a JSON object
        tester->executeWaitVerifyContent(command, "402 expectedData is not a valid JSON Object");

        command["expectedData"] = "{\"activity\":1,\"activity\":1}";
        tester->executeWaitVerifyContent(command, "402 expectedData is not a valid JSON Object");

        command["expectedData"] = expectedFinishData;
        command["data"] = "[]";
        tester->executeWaitVerifyContent(command, "402 Data is not a valid JSON Object");

        // Given a duplicate enqueue that replaces the activity while the original worker remains active
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "finishComparedData";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        // When the original worker finishes with output from its stale activity
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishJobID;
        command["expectedData"] = expectedFinishData;
        command["data"] = "{\"activity\":1,\"worker\":true}";
        tester->executeWaitVerifyContent(command);

        // Then Bedrock queues the newer activity and discards stale worker output
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockRerunIfDataChanged'), "
                       "JSON_EXTRACT(data, '$.activity'), JSON_EXTRACT(data, '$.worker') "
                       "FROM jobs WHERE jobID=" + finishJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "1");
        ASSERT_EQUAL(result[0][2], "2");
        ASSERT_EQUAL(result[0][3], "");

        // Given the subsequent worker receives the preserved activity
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "finishComparedData";
        runningJob = tester->executeWaitVerifyContentTable(command);

        // When that worker finishes with its current snapshot
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishJobID;
        command["expectedData"] = SDecodeBase64(runningJob.at("expectedDataBase64"));
        tester->executeWaitVerifyContent(command);

        // Then Bedrock completes the row because no newer activity exists
        tester->readDB("SELECT COUNT(1) FROM jobs WHERE jobID=" + finishJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "0");

        // Given an opted-in worker whose row receives newer activity and queue priority
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "retryComparedData";
        command["data"] = "{\"activity\":1,\"timeoutRetries\":0}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string retryJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "retryComparedData";
        runningJob = tester->executeWaitVerifyContentTable(command);
        const string expectedRetryData = SDecodeBase64(runningJob.at("expectedDataBase64"));

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "retryComparedData";
        command["data"] = "{\"activity\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        command["jobPriority"] = "750";
        tester->executeWaitVerifyContent(command);

        // When the stale worker requests a retry with different output and scheduling attributes
        command.clear();
        command.methodLine = "RetryJob";
        command["jobID"] = retryJobID;
        command["expectedData"] = expectedRetryData;
        command["nextRun"] = "2042-04-02 00:42:42";
        command["ignoreRepeat"] = "true";
        command["name"] = "staleName";
        command["jobPriority"] = "1000";
        command["data"] = "{\"activity\":1,\"timeoutRetries\":1,\"worker\":true}";
        tester->executeWaitVerifyContent(command);

        // Then Bedrock preserves conflicting enqueue data, applies other worker changes, and honors the retry time
        tester->readDB("SELECT state, name, nextRun, priority, JSON_EXTRACT(data, '$.activity'), "
                       "JSON_EXTRACT(data, '$.timeoutRetries'), JSON_EXTRACT(data, '$.worker') "
                       "FROM jobs WHERE jobID=" + retryJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "retryComparedData");
        ASSERT_EQUAL(result[0][2], "2042-04-02 00:42:42");
        ASSERT_EQUAL(result[0][3], "750");
        ASSERT_EQUAL(result[0][4], "2");
        ASSERT_EQUAL(result[0][5], "1");
        ASSERT_EQUAL(result[0][6], "1");

        // Given a legacy unique worker that started before its job enabled rerun-if-data-changed
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

        // When the legacy worker finishes with the snapshot from its original dequeue
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = legacyJobID;
        command["expectedData"] = legacyJob["data"];
        tester->executeWaitVerifyContent(command);

        // Then Bedrock queues the newer activity because opt-in and comparison occur atomically
        tester->readDB("SELECT state, JSON_EXTRACT(data, '$._bedrockRerunIfDataChanged'), JSON_EXTRACT(data, '$.activity') "
                       "FROM jobs WHERE jobID=" + legacyJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "1");
        ASSERT_EQUAL(result[0][2], "2");

        // Given an opted-in job that receives newer activity from a duplicate enqueue
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

        // When an old worker manager finishes without expectedData
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = legacyCompletionJobID;
        tester->executeWaitVerifyContent(command);

        // Then Bedrock uses legacy completion because rolling deployments require backward compatibility
        tester->readDB("SELECT COUNT(1) FROM jobs WHERE jobID=" + legacyCompletionJobID + ";", result);
        ASSERT_EQUAL(result[0][0], "0");
    }

    void uniqueAsRetryStrictDataAndSnapshot()
    {
        SData command("CreateJob");
        command["name"] = "strictDuplicateData";
        command["data"] = "{\"value\":1,\"value\":2}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command, "402 Data is not a valid JSON Object");

        command["name"] = "strictNestedDuplicateData";
        command["data"] = "{\"nested\":{\"value\":1,\"value\":2}}";
        tester->executeWaitVerifyContent(command, "402 Data is not a valid JSON Object");

        command["name"] = "strictArrayData";
        command["data"] = "[]";
        tester->executeWaitVerifyContent(command, "402 Data is not a valid JSON Object");

        command["name"] = "strictWhitespaceObject";
        command["data"] = " { } ";
        const string whitespaceJobID = tester->executeWaitVerifyContentTable(command)["jobID"];
        ASSERT_GREATER_THAN(SToInt64(whitespaceJobID), 0);

        const string preciseData =
            "{\"emptyObject\":{},\"uint64\":18446744073709551615,"
            "\"preciseFloat\":9007199254740993.0,\"underflow\":1e-324,"
            "\"hugeExponent\":1e999999999999999999999999999999999999,\"nul\\u0000key\":true}";
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "exactSnapshot";
        command["data"] = preciseData;
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "exactSnapshot";
        const STable runningJob = tester->executeWaitVerifyContentTable(command);
        ASSERT_TRUE(SContains(runningJob, "expectedDataBase64"));
        const string exactSnapshot = SDecodeBase64(runningJob.at("expectedDataBase64"));
        ASSERT_TRUE(SJSONEquals(preciseData, exactSnapshot));
        ASSERT_TRUE(exactSnapshot.find("18446744073709551615") != string::npos);
        ASSERT_TRUE(exactSnapshot.find("9007199254740993.0") != string::npos);
        ASSERT_TRUE(exactSnapshot.find("1e-324") != string::npos);
        ASSERT_TRUE(exactSnapshot.find("1e999999999999999999999999999999999999") != string::npos);
        ASSERT_TRUE(exactSnapshot.find("nul\\u0000key") != string::npos);

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "invalidStoredData";
        command["data"] = "{\"value\":1}";
        command["unique"] = "true";
        const string invalidStoredJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "Query";
        command["query"] = "UPDATE jobs SET data = '{\"value\":1,\"value\":2}' WHERE jobID = " + invalidStoredJobID + ";";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "invalidStoredData";
        command["data"] = "{\"replacement\":true}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command, "402 Cannot enable uniqueAsRetry on invalid stored data");

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "corruptOptedData";
        command["data"] = "{\"value\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string corruptJobID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "Query";
        command["query"] = "UPDATE jobs SET data = "
            "'{\"_bedrockRerunIfDataChanged\":true,\"value\":1,\"value\":2}' WHERE jobID = " +
            corruptJobID + ";";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "corruptOptedData";
        command["data"] = "{\"value\":3}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        tester->executeWaitVerifyContent(command, "500 Opted-in job contains invalid JSON data");

        command.clear();
        command.methodLine = "QueryJob";
        command["jobID"] = corruptJobID;
        tester->executeWaitVerifyContent(command, "500 Opted-in job contains invalid JSON data");

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "corruptOptedData";
        tester->executeWaitVerifyContent(command, "500 Opted-in job contains invalid JSON data");

        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = corruptJobID;
        tester->executeWaitVerifyContent(command, "500 Opted-in job contains invalid JSON data");
    }

    void uniqueAsRetryCannotOwnChildren()
    {
        // Given an opted-in job whose retry lifecycle can requeue its row
        SData command("CreateJob");
        command["name"] = "optedParent";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";
        const string optedParentID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "optedParent";
        tester->executeWaitVerifyContent(command);

        // When a caller tries to attach a child to that job
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "rejectedChild";
        command["parentJobID"] = optedParentID;

        // Then Bedrock rejects the child because requeue semantics cannot preserve parent completion behavior
        tester->executeWaitVerifyContent(command, "405 uniqueAsRetry jobs cannot own child jobs");

        // Given a unique job that already owns a child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "existingParent";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        const string existingParentID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "existingParent";
        tester->executeWaitVerifyContent(command);

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "existingChild";
        command["parentJobID"] = existingParentID;
        tester->executeWaitVerifyContent(command);

        // When a caller tries to enable rerun-if-data-changed on the parent
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "existingParent";
        command["data"] = "{\"activity\":1}";
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";

        // Then Bedrock rejects the opt-in because existing child state requires normal parent completion
        tester->executeWaitVerifyContent(command, "405 uniqueAsRetry jobs cannot own child jobs");

        // Given a normal parent whose completion behavior supports child jobs
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "allowedParent";
        const string allowedParentID = tester->executeWaitVerifyContentTable(command)["jobID"];

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "allowedParent";
        tester->executeWaitVerifyContent(command);

        // When a caller creates an opted-in job as a child of that parent
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "allowedOptedChild";
        command["parentJobID"] = allowedParentID;
        command["unique"] = "true";
        command["uniqueAsRetry"] = "true";

        // Then Bedrock accepts the child because the opted-in job does not own the relationship
        ASSERT_GREATER_THAN(SToInt64(tester->executeWaitVerifyContentTable(command)["jobID"]), 0);
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
