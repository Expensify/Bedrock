#include <libstuff/SQResult.h>
#include <libstuff/SData.h>
#include <libstuff/JSON/Value.h>
#include <test/lib/BedrockTester.h>

struct CancelJobTest : tpunit::TestFixture
{
    CancelJobTest()
        : tpunit::TestFixture("CancelJob",
                              BEFORE_CLASS(CancelJobTest::setupClass),
                              TEST(CancelJobTest::cancelNonExistentJob),
                              TEST(CancelJobTest::cancelJobWithChild),
                              TEST(CancelJobTest::cancelRunningJob),
                              TEST(CancelJobTest::cancelFinishedJob),
                              TEST(CancelJobTest::cancelPausedJob),
                              TEST(CancelJobTest::cancelChildJob),
                              TEST(CancelJobTest::cancelJobWithoutParent),
                              TEST(CancelJobTest::cancelJobWithSiblings),
                              TEST(CancelJobTest::cancelTreeDuringRetry),
                              TEST(CancelJobTest::cancelTreeBeforeFinish),
                              TEST(CancelJobTest::cancelTreeWhilePaused),
                              TEST(CancelJobTest::cancelTreeWithRunningChild),
                              TEST(CancelJobTest::cancelTreeWithRunqueuedChild),
                              TEST(CancelJobTest::cancelTreeAfterWorkerFailure),
                              TEST(CancelJobTest::cancelTreeAfterParentFailed),
                              TEST(CancelJobTest::cancelTreeWithFailedChild),
                              TEST(CancelJobTest::cancelTreeRejectsInvalidTargets),
                              TEST(CancelJobTest::cancelMarkerCannotBeSupplied),
                              AFTER(CancelJobTest::tearDown),
                              AFTER_CLASS(CancelJobTest::tearDownClass))
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

    string startParent()
    {
        SData create("CreateJob");
        create["name"] = "tree-parent";
        create["data"] = R"({"receiptID":7})";
        create["retryAfter"] = "+5 MINUTES";
        const string parentID = tester->executeWaitVerifyContentTable(create)["jobID"];
        SData get("GetJob");
        get["name"] = "tree-parent";
        tester->executeWaitVerifyContent(get);
        return parentID;
    }

    string createChild(const string& parentID, bool withRetry = false)
    {
        SData create("CreateJob");
        create["name"] = "tree-child";
        create["parentJobID"] = parentID;
        if (withRetry) {
            create["retryAfter"] = "+5 MINUTES";
        }
        return tester->executeWaitVerifyContentTable(create)["jobID"];
    }

    string getState(const string& jobID)
    {
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + SQ(jobID) + ";", result);
        return result.empty() ? "" : result[0]["state"];
    }

    void cancelTree(const string& parentID)
    {
        SData cancel("CancelJobTree");
        cancel["jobID"] = parentID;
        tester->executeWaitVerifyContent(cancel);
    }

    void finishJob(const string& jobID, const string& data = "")
    {
        SData finish("FinishJob");
        finish["jobID"] = jobID;
        if (!data.empty()) {
            finish["data"] = data;
        }
        tester->executeWaitVerifyContent(finish);
    }

    void dequeueParent()
    {
        SData get("GetJob");
        get["name"] = "tree-parent";
        tester->executeWaitVerifyContent(get);
    }

    void assertCancelledParentData(const string& parentID)
    {
        SQResult result;
        tester->readDB("SELECT IIF(JSON_VALID(data), JSON_TYPE(data, '$._bedrockCancelRequested'), '') AS marker, "
                       "IIF(JSON_VALID(data), JSON_TYPE(data, '$.wasCancelled'), '') AS cancelled "
                       "FROM jobs WHERE jobID = " + SQ(parentID) + ";", result);
        ASSERT_FALSE(result.empty());
        ASSERT_EQUAL(result[0]["marker"], "true");
        ASSERT_EQUAL(result[0]["cancelled"], "true");
    }

    void cancelTreeDuringRetry()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        cancelTree(parentID);
        ASSERT_EQUAL(getState(parentID), "RUNQUEUED");
        ASSERT_EQUAL(getState(childID), "CANCELLED");

        SData create("CreateJob");
        create["name"] = "late-child";
        create["parentJobID"] = parentID;
        tester->executeWaitVerifyContent(create, "405 Parent job cancellation requested");
        SData createJobs("CreateJobs");
        createJobs["jobs"] = "[{\"name\":\"late-child\",\"parentJobID\":" + parentID + "}]";
        tester->executeWaitVerifyContent(createJobs, "405 Parent job cancellation requested");

        SData update("UpdateJob");
        update["jobID"] = parentID;
        update["data"] = R"({"receiptID":7})";
        tester->executeWaitVerifyContent(update);
        assertCancelledParentData(parentID);

        SData retry("RetryJob");
        retry["jobID"] = parentID;
        retry["delay"] = "120";
        retry["data"] = R"({"receiptID":7,"pending":[1,2]})";
        tester->executeWaitVerifyContent(retry);
        ASSERT_EQUAL(getState(parentID), "QUEUED");
        assertCancelledParentData(parentID);
        dequeueParent();
        finishJob(parentID, R"({"receiptID":7})");
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
    }

    void cancelTreeBeforeFinish()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        cancelTree(parentID);
        SData recreate("CreateJob");
        recreate["name"] = "tree-parent";
        recreate["unique"] = "true";
        recreate["data"] = R"({"receiptID":7})";
        tester->executeWaitVerifyContent(recreate, "405 Existing job cancellation requested");
        finishJob(parentID, R"({"receiptID":7})");
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
        ASSERT_FALSE(tester->executeWaitVerifyContentTable(recreate)["jobID"].empty());
    }

    void cancelTreeWhilePaused()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        finishJob(parentID);
        ASSERT_EQUAL(getState(parentID), "PAUSED");
        ASSERT_EQUAL(getState(childID), "QUEUED");
        cancelTree(parentID);
        ASSERT_EQUAL(getState(parentID), "QUEUED");
        ASSERT_EQUAL(getState(childID), "CANCELLED");
        dequeueParent();
        assertCancelledParentData(parentID);
        finishJob(parentID);
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
    }

    void cancelTreeWithRunningChild()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        finishJob(parentID);
        SData getChild("GetJob");
        getChild["name"] = "tree-child";
        tester->executeWaitVerifyContent(getChild);
        cancelTree(parentID);
        ASSERT_EQUAL(getState(parentID), "PAUSED");
        ASSERT_EQUAL(getState(childID), "RUNNING");
        finishJob(childID);
        ASSERT_EQUAL(getState(parentID), "QUEUED");
        dequeueParent();
        assertCancelledParentData(parentID);
        finishJob(parentID);
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
    }

    void cancelTreeWithRunqueuedChild()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID, true);
        finishJob(parentID);
        SData getChild("GetJob");
        getChild["name"] = "tree-child";
        tester->executeWaitVerifyContent(getChild);
        ASSERT_EQUAL(getState(childID), "RUNQUEUED");
        cancelTree(parentID);
        ASSERT_EQUAL(getState(parentID), "QUEUED");
        ASSERT_EQUAL(getState(childID), "CANCELLED");

        SData staleFinish("FinishJob");
        staleFinish["jobID"] = childID;
        tester->executeWaitVerifyContent(staleFinish, "405 Can only retry/finish RUNNING and RUNQUEUED jobs");
        dequeueParent();
        finishJob(parentID);
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
    }

    void cancelTreeAfterWorkerFailure()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        cancelTree(parentID);
        SData fail("FailJob");
        fail["jobID"] = parentID;
        tester->executeWaitVerifyContent(fail);
        ASSERT_EQUAL(getState(parentID), "QUEUED");
        assertCancelledParentData(parentID);
        dequeueParent();
        finishJob(parentID);
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
    }

    void cancelTreeAfterParentFailed()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        SData fail("FailJob");
        fail["jobID"] = parentID;
        tester->executeWaitVerifyContent(fail);
        ASSERT_EQUAL(getState(parentID), "FAILED");
        ASSERT_EQUAL(getState(childID), "PAUSED");
        cancelTree(parentID);
        ASSERT_EQUAL(getState(parentID), "QUEUED");
        ASSERT_EQUAL(getState(childID), "CANCELLED");
        dequeueParent();
        finishJob(parentID);
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
    }

    void cancelTreeWithFailedChild()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        finishJob(parentID);
        SData getChild("GetJob");
        getChild["name"] = "tree-child";
        tester->executeWaitVerifyContent(getChild);
        SData fail("FailJob");
        fail["jobID"] = childID;
        tester->executeWaitVerifyContent(fail);
        ASSERT_EQUAL(getState(parentID), "PAUSED");
        ASSERT_EQUAL(getState(childID), "FAILED");
        cancelTree(parentID);
        ASSERT_EQUAL(getState(parentID), "QUEUED");
        ASSERT_EQUAL(getState(childID), "CANCELLED");
        dequeueParent();
        finishJob(parentID);
        ASSERT_EQUAL(getState(parentID), "");
        ASSERT_EQUAL(getState(childID), "");
    }

    void cancelTreeRejectsInvalidTargets()
    {
        const string parentID = startParent();
        const string childID = createChild(parentID);
        SData cancel("CancelJobTree");
        cancel["jobID"] = childID;
        tester->executeWaitVerifyContent(cancel, "405 Can only cancel non-repeating root jobs with retryAfter");

        SData create("CreateJob");
        create["name"] = "repeating";
        create["repeat"] = "HOURLY";
        const string repeatingID = tester->executeWaitVerifyContentTable(create)["jobID"];
        cancel["jobID"] = repeatingID;
        tester->executeWaitVerifyContent(cancel, "405 Can only cancel non-repeating root jobs with retryAfter");

        create.clear();
        create.methodLine = "CreateJob";
        create["name"] = "without-retry";
        const string withoutRetryID = tester->executeWaitVerifyContentTable(create)["jobID"];
        cancel["jobID"] = withoutRetryID;
        tester->executeWaitVerifyContent(cancel, "405 Can only cancel non-repeating root jobs with retryAfter");
    }

    void cancelMarkerCannotBeSupplied()
    {
        SData create("CreateJob");
        create["name"] = "tree-parent";
        create["data"] = R"({"_bedrockCancelRequested":true,"_bedrockCancelRequested\u0000caller":"kept"})";
        const string parentID = tester->executeWaitVerifyContentTable(create)["jobID"];
        SData get("GetJob");
        get["name"] = "tree-parent";
        const JSON::Value job = JSON::Value::parse(tester->executeWaitVerifyContent(get));
        ASSERT_FALSE(job["data"].hasMember("_bedrockCancelRequested"));
        ASSERT_EQUAL(job["data"]["_bedrockCancelRequested\0caller"s].getString(), "kept");
        ASSERT_FALSE(createChild(parentID).empty());
    }

    // Cannot cancel a job that doesn't exist
    void cancelNonExistentJob()
    {
        SData command("CancelJob");
        command["jobID"] = "1";
        tester->executeWaitVerifyContent(command, "404 No job with this jobID");
    }

    // Cannot cancel a job with children
    void cancelJobWithChild()
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

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get the child and finish it to put the parent in the QUEUED state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWaitVerifyContent(command);

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

        // Finish the known child.
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert parent is in QUEUED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Cannot finish a job with a child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command, "404 Invalid jobID - Cannot cancel a job with children");
    }

    // Ignore canceljob for RUNNING jobs
    void cancelRunningJob()
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
        string jobID = response["jobID"];

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get the child job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWaitVerifyContent(command);

        // Assert job is in RUNNING state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Cannot finish a job in RUNNING state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
    }

    // Ignore canceljob for FINISHED jobs
    void cancelFinishedJob()
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

        // Finish the parent
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get the child and finish it to put the child in the FINISHED state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "child";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job is in FINISHED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "FINISHED");

        // Cannot finish a job in FINISHED state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "FINISHED");
    }

    // Ignore canceljob for PAUSED jobs
    void cancelPausedJob()
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

        // Assert job is in PAUSED state
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");

        // Cannot finish a job in PAUSED state
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is unchanged
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");
    }

    // Cancel a child job
    void cancelChildJob()
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

        // Create a sibling
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "sibling";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string siblingID = response["jobID"];

        // Finish the parent to put the children in the QUEUED state
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Get one the children
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "sibling";
        tester->executeWaitVerifyContent(command);

        // Cancel the QUEUED child job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Assert job state is cancelled, but sibling is still running
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + childID + ";", result);
        ASSERT_EQUAL(result[0][0], "CANCELLED");

        tester->readDB("SELECT state FROM jobs WHERE jobID = " + siblingID + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");
    }

    // Cancel a child job that doesn't have a parent
    void cancelJobWithoutParent()
    {
        // Create the job
        SData command("CreateJob");
        command["name"] = "orphanChild";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];

        // Try cancelling the job
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = jobID;
        tester->executeWaitVerifyContent(command, "404 Invalid jobID - Cannot cancel a job without a parent");
    }

    // Cancel a child job that's the last child job of the parent
    void cancelJobWithSiblings()
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

        // Create one child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string childID = response["jobID"];

        // Create another child
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "child2";
        command["parentJobID"] = parentID;
        response = tester->executeWaitVerifyContentTable(command);
        string childID2 = response["jobID"];

        // Finish the parent to put the child in the QUEUED state
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Cancel one child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID;
        tester->executeWaitVerifyContent(command);

        // Parent should still be PAUSED
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "PAUSED");

        // The parent may have other children from mock requests, delete them.
        command.clear();
        command.methodLine = "Query";
        command["Query"] = "DELETE FROM jobs WHERE parentJobID = " + parentID + " AND JSON_EXTRACT(data, '$.mockRequest') IS NOT NULL;";
        tester->executeWaitVerifyContent(command);

        // Cancel the last child
        command.clear();
        command.methodLine = "CancelJob";
        command["jobID"] = childID2;
        tester->executeWaitVerifyContent(command);

        // Parent should be queued
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + parentID + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }
} __CancelJobTest;
