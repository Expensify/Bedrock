#include <libstuff/SQResult.h>
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct FinishJobTest : tpunit::TestFixture {
    FinishJobTest() : tpunit::TestFixture("FinishJob") {
        registerTests(BEFORE_CLASS(FinishJobTest::setupClass),
                        TEST(FinishJobTest::nonExistentJob),
                        TEST(FinishJobTest::notInRunningState),
                        TEST(FinishJobTest::parentIsNotPaused),
                        TEST(FinishJobTest::removeFinishedAndCancelledChildren),
                        TEST(FinishJobTest::updateData),
                        TEST(FinishJobTest::finishingParentUnPausesChildren),
                        TEST(FinishJobTest::deleteFinishedJobWithNoChildren),
                        TEST(FinishJobTest::hasRepeat),
                        TEST(FinishJobTest::inRunqueuedState),
                        TEST(FinishJobTest::hasRepeatWithDelay),
                        TEST(FinishJobTest::hasDelay),
                        TEST(FinishJobTest::hasRepeatWithNextRun),
                        TEST(FinishJobTest::hasDataDelete),
                        TEST(FinishJobTest::hasNextRun),
                        TEST(FinishJobTest::simpleFinishJobWithHttp),
                        AFTER(FinishJobTest::tearDown),
                        AFTER_CLASS(FinishJobTest::tearDownClass));
    }

    BedrockClusterTester* clusterTester;
    BedrockTester* tester;

    void setupClass() {
        clusterTester = new BedrockClusterTester(ClusterSize::THREE_NODE_CLUSTER, {});
        tester = &(clusterTester->getTester(1));
    }

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        clusterTester->getTester(0).executeWaitVerifyContent(command);
    }

    void tearDownClass() {
        delete clusterTester;
    }

    // Throw an error if the job doesn't exist
    void nonExistentJob() {
        SData command("FinishJob");
        command["jobID"] = "1";
        tester->executeWaitVerifyContent(command, "404 No job with this jobID");
    }

    // Throw an error if the job is not in RUNNING state
    void notInRunningState() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "405 Can only retry/finish RUNNING and RUNQUEUED jobs");
    }

    // If job has a parentID, the parent should be paused
    void parentIsNotPaused() {
        // Create the parent
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

        // It's not possible to put the child in the QUEUED state without the parent being paused
        // and a child cannot being the RUNNING state without first being the QUEUED state
        // but we check for this to make sure something funky didn't occur.
        // We'll manually put the child in the RUNNING state to hit this condition
        command.clear();
        command.methodLine = "Query";
        command["query"] = "UPDATE jobs SET state = 'RUNNING' WHERE jobID = " + childID + ";";
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Finish the child
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command, "405 Can only retry/finish child job when parent is PAUSED");
    }

    // Child jobs that are in the FINISHED or CANCELLED state should be deleted when the parent is finished
    void removeFinishedAndCancelledChildren() {
        // Create the parent
        SData command("CreateJob");
        command["name"] = "parent";
        STable response = clusterTester->getTester(0).executeWaitVerifyContentTable(command);
        string parentID = response["jobID"];

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Create the children
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_finished";
        command["parentJobID"] = parentID;
        response = clusterTester->getTester(0).executeWaitVerifyContentTable(command);
        string finishedChildID = response["jobID"];
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_cancelled";
        command["parentJobID"] = parentID;
        response = clusterTester->getTester(0).executeWaitVerifyContentTable(command);
        string cancelledChildID = response["jobID"];
        command.clear();

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Get the child job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child_finished";
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Cancel a child
        // if this goes 2nd this doesn't requeue the parent job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = cancelledChildID;
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Finish a child
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = finishedChildID;
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Confirm the parent is set to QUEUED
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Finish the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        clusterTester->getTester(0).executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Confirm that the FINISHED and CANCELLED children are deleted
        clusterTester->getTester(0).readDB("SELECT count(*) FROM jobs WHERE jobID != " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL;", result);
        ASSERT_EQUAL(SToInt(result[0][0]), 0);
    }

    // Update the job data if new data is passed
    void updateData() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        STable data;
        data["foo"] = "bar";
        data["bar"] = "foo";
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["data"] = SComposeJSONObject(data);
        tester->executeWaitVerifyContent(command);

        // Confirm the data updated
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT data FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], SComposeJSONObject(data));
    }

    void finishingParentUnPausesChildren() {
        // Create the parent
        SData command("CreateJob");
        command["name"] = "parent";
        STable response = tester->executeWaitVerifyContentTable(command);
        string parentID = response["jobID"];

        // Get the parent
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "parent";
        tester->executeWaitVerifyContent(command);

        // Create the children
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_finished";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string finishedChildID = response["jobID"];
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child_cancelled";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string cancelledChildID = response["jobID"];
        command.clear();

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        clusterTester->getTester(0).executeWaitVerifyContent(command);

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Confirm that the parent is in the PAUSED state and the children are in the QUEUED state
        SQResult result;
        list<string> ids = {parentID, finishedChildID, cancelledChildID};
        clusterTester->getTester(0).readDB("SELECT jobID, state FROM jobs WHERE jobID IN(" + SComposeList(ids) + ");", result);
        ASSERT_EQUAL(result.rows.size(), 3);
        for (auto& row : result.rows) {
            if (row[0] == parentID) {
                ASSERT_EQUAL(row[1], "PAUSED");
            } else if (row[0] == finishedChildID) {
                ASSERT_EQUAL(row[1], "QUEUED");
            } else if (row[0] == cancelledChildID) {
                ASSERT_EQUAL(row[1], "QUEUED");
            } else {
                ASSERT_TRUE(false);
            }
        }
    }

    void deleteFinishedJobWithNoChildren() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confirm the job was deleted
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT * FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_TRUE(result.empty());
    }

    // Cannot retry with a negative delay
    void negativeDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["delay"] = "-5";
        tester->executeWaitVerifyContent(command, "402 Must specify a non-negative delay when retrying");
    }

    // Finish with a positive delay and confirm nextRun is updated appropriately
    void positiveDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the nextRun value
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        string originalNextRun = result[0][0];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWaitVerifyContent(command);

        // Assert the new nextRun time is 5 seconds after the original nextRun time
        clusterTester->getTester(0).readDB("SELECT nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        time_t currentNextRunTime = JobTestHelper::getTimestampForDateTimeString(result[0][0]);
        time_t originalNextRunTime = JobTestHelper::getTimestampForDateTimeString(originalNextRun);
        ASSERT_EQUAL(difftime(currentNextRunTime, originalNextRunTime), 5);
    }

    // Finish a job with a repeat
    void hasRepeat() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is in 1 hour from the created time
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT lastRun, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        time_t createdTime = JobTestHelper::getTimestampForDateTimeString(result[0][0]);
        time_t nextRunTime = JobTestHelper::getTimestampForDateTimeString(result[0][1]);
        ASSERT_EQUAL(difftime(nextRunTime, createdTime), 3600);
    }

    // Finish job in RUNQUEUED state
    void inRunqueuedState() {
        // Create a job
        SData command("CreateJob");
        command["name"] = "job";
        command["retryAfter"] = "+1 SECOND";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Confirm the job is in RUNQUEUED
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_EQUAL(result[0][0], "RUNQUEUED");

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Finishing the job should remove it from the table
        clusterTester->getTester(0).readDB("SELECT * FROM jobs WHERE jobID = " + jobID + ";",  result);
        ASSERT_TRUE(result.empty());
    }

    // FinishJob with repeat should ignore the 'delay' parameter
    void hasRepeatWithDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is in 1 hour, not in the 5 second delay
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT lastRun, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_EQUAL(difftime(JobTestHelper::getTimestampForDateTimeString(result[0][1]), JobTestHelper::getTimestampForDateTimeString(result[0][0])), 3600);
    }

    // FinishJob should ignore the 'delay' parameter
    void hasDelay() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["delay"] = "5";
        tester->executeWaitVerifyContent(command);

        // Confirm the job was deleted instead of being rescheduled
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT * FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_TRUE(result.empty());
    }

    // FinishJob with repeat should ignore the 'nextRun' parameter
    void hasRepeatWithNextRun() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["nextRun"] = "2017-09-07 23:11:11";
        tester->executeWaitVerifyContent(command);

        // Confirm nextRun is in 1 hour, not in the given nextRun time
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT lastRun, nextRun FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_EQUAL(difftime(JobTestHelper::getTimestampForDateTimeString(result[0][1]), JobTestHelper::getTimestampForDateTimeString(result[0][0])), 3600);
    }

    // FinishJob should delete any job with data.delete = true
    void hasDataDelete() {
        // Create the recurring job
        SData command("CreateJob");
        command["name"] = "job";
        command["repeat"] = "STARTED, +1 HOUR";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        STable data;
        data["delete"] = true;
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["data"] = SComposeJSONObject(data);
        tester->executeWaitVerifyContent(command);

        // Confirm the job was deleted instead of being rescheduled
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT * FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_TRUE(result.empty());
    }

    // FinishJob should ignore the 'nextRun' parameter
    void hasNextRun() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID;
        command["nextRun"] = "2017-09-07 23:11:11";
        tester->executeWaitVerifyContent(command);


        // Confirm the job was deleted instead of being rescheduled
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT * FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_TRUE(result.empty());
    }

    void simpleFinishJobWithHttp() {
        // Create the job
        SData command("CreateJob");
        command["name"] = "job";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Get the job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "job";
        tester->executeWaitVerifyContent(command);

        // Finish it
        command.clear();
        command.methodLine = "FinishJob / HTTP/1.1";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Confirm the job was deleted
        SQResult result;
        clusterTester->getTester(0).readDB("SELECT * FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_TRUE(result.empty());
    }
} __FinishJobTest;
