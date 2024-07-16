#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>

struct CreateJobsTest : tpunit::TestFixture {
    CreateJobsTest() : tpunit::TestFixture("CreateJobs") {
        registerTests(BEFORE_CLASS(CreateJobsTest::setupClass),
                      TEST(CreateJobsTest::create),
                      TEST(CreateJobsTest::createWithHttp),
                      TEST(CreateJobsTest::createWithInvalidJson),
                      TEST(CreateJobsTest::createWithParentIDNotRunning),
                      TEST(CreateJobsTest::createWithParentMocked),
                      TEST(CreateJobsTest::createUniqueChildWithWrongParent),
                      AFTER(CreateJobsTest::tearDown),
                      AFTER_CLASS(CreateJobsTest::tearDownClass));
    }

    BedrockTester* tester;

    string _generateCreateJobContentJSON() {
        STable job1Content;
        STable data1;
        data1["blabla"] = "blabla";
        job1Content["name"] = "testCreate";
        job1Content["data"] = SComposeJSONObject(data1);
        job1Content["repeat"] = "SCHEDULED, +1 HOUR";

        STable job2Content;
        STable data2;
        data2["nope"] = "nope";
        job2Content["name"] = "testCreate2";
        job2Content["data"] = SComposeJSONObject(data2);

        vector<string> jobs;
        jobs.push_back(SComposeJSONObject(job1Content));
        jobs.push_back(SComposeJSONObject(job2Content));

        return SComposeJSONArray(jobs);
    }

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
        command["jobs"] = _generateCreateJobContentJSON();
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobIDList = SParseJSONArray(response["jobIDs"]);
        ASSERT_EQUAL(jobIDList.size(), 2);
        string jobID1 = jobIDList.front();
        string jobID2 = jobIDList.back();
        ASSERT_NOT_EQUAL(jobID1, jobID2);
    }

    void createWithHttp() {
        SData command("CreateJobs / HTTP/1.1");
        command["jobs"] = _generateCreateJobContentJSON();
        STable response = tester->executeWaitVerifyContentTable(command);
        list<string> jobIDList = SParseJSONArray(response["jobIDs"]);
        ASSERT_EQUAL(jobIDList.size(), 2);
        string jobID1 = jobIDList.front();
        string jobID2 = jobIDList.back();
        ASSERT_NOT_EQUAL(jobID1, jobID2);
    }

    void createWithInvalidJson() {
        SData command("CreateJobs");
        command["jobs"] = _generateCreateJobContentJSON() + "}";
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

        STable job1Content;
        STable data1;
        data1["blabla"] = "blabla";
        job1Content["name"] = "testCreate";
        job1Content["data"] = SComposeJSONObject(data1);
        job1Content["repeat"] = "SCHEDULED, +1 HOUR";
        job1Content["parentJobID"] = response["jobID"];

        STable job2Content;
        STable data2;
        data2["nope"] = "nope";
        job2Content["name"] = "testCreate2";
        job2Content["data"] = SComposeJSONObject(data2);

        vector<string> jobs;
        jobs.push_back(SComposeJSONObject(job1Content));
        jobs.push_back(SComposeJSONObject(job2Content));
        command["jobs"] = SComposeJSONArray(jobs);
        tester->executeWaitVerifyContent(command, "405 Can only create child job when parent is RUNNING, RUNQUEUED or PAUSED");
    }

    void createWithParentMocked() {

        // Create a mocked parent.
        SData command("CreateJob");
        command["name"] = "createWithParentMocked";
        command["mockRequest"] = "true";
        string response = tester->executeWaitVerifyContent(command);
        STable responseJSON = SParseJSONObject(response);
        string parentID = responseJSON["jobID"];

        // Get the parent (to set it running). mockRequest must be set or we'll get a non-mocked parent.
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "createWithParentMocked";
        command["mockRequest"] = "true";
        response = tester->executeWaitVerifyContent(command);

        // Now try to create two child jobs. These aren't explicitly mocked.
        command.clear();
        command.methodLine = "CreateJobs";

        STable job1Content;
        STable data1;
        data1["blabla"] = "blabla";
        job1Content["name"] = "createWithParentMocked_child1";
        job1Content["data"] = SComposeJSONObject(data1);
        job1Content["parentJobID"] = parentID;

        STable job2Content;
        STable data2;
        data2["nope"] = "nope";
        job2Content["name"] = "createWithParentMocked_child2";
        job2Content["data"] = SComposeJSONObject(data2);
        job1Content["parentJobID"] = parentID;

        // Send the command to create the children. Note that they're not explicitly mocked.
        vector<string> jobs;
        jobs.push_back(SComposeJSONObject(job1Content));
        jobs.push_back(SComposeJSONObject(job2Content));
        command["jobs"] = SComposeJSONArray(jobs);
        response = tester->executeWaitVerifyContent(command);

        // Finish the parent so that our children are queued to run.
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = parentID;
        tester->executeWaitVerifyContent(command);

        // Verify the children came back mocked. We have to ask for them with the `mocked` flag or we won't get them.
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "createWithParentMocked_child1";
        command["mockRequest"] = "true";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        int64_t child1ID = stol(response2["jobID"]);
        responseJSON = SParseJSONObject(response2["data"]);
        ASSERT_TRUE(responseJSON.find("mockRequest") != responseJSON.end());

        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "createWithParentMocked_child2";
        command["mockRequest"] = "true";
        response2 = tester->executeWaitVerifyContentTable(command);
        int64_t child2ID = stol(response2["jobID"]);
        responseJSON = SParseJSONObject(response2["data"]);
        ASSERT_TRUE(responseJSON.find("mockRequest") != responseJSON.end());


        // Finish the children.
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = to_string(child1ID);
        tester->executeWaitVerifyContent(command);
        command["jobID"] = to_string(child2ID);
        tester->executeWaitVerifyContent(command);

        // Now we can finish the parent.
        command["jobID"] = parentID;
        response = tester->executeWaitVerifyContent(command);

        // Now make sure these are gone.
        list<int64_t> jobIDs = {child1ID, child2ID, stol(parentID)};
        SQResult result;
        string query = "SELECT jobID, state FROM jobs WHERE jobID in (" + SQList(jobIDs) + ");";
        tester->readDB(query, result);

        ASSERT_EQUAL(result.rows.size(), 0);
    }

    void createUniqueChildWithWrongParent()
    {
        // Create 2 parents
        SData command("CreateJob");
        command["name"] = "createWithParent1";
        string response = tester->executeWaitVerifyContent(command);
        STable responseJSON = SParseJSONObject(response);
        string parentID1 = responseJSON["jobID"];
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "createWithParent2";
        response = tester->executeWaitVerifyContent(command);
        responseJSON = SParseJSONObject(response);
        string parentID2 = responseJSON["jobID"];

        // Get the parents (to set it running)
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "createWithParent1";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "createWithParent2";
        tester->executeWaitVerifyContent(command);

        // Send the command to create the child in parent1
        command.clear();
        command.methodLine = "CreateJobs";
        STable job1Content;
        STable data1;
        data1["blabla"] = "blabla";
        job1Content["name"] = "createWithParent_child";
        job1Content["data"] = SComposeJSONObject(data1);
        job1Content["parentJobID"] = parentID1;
        vector<string> jobs;
        jobs.push_back(SComposeJSONObject(job1Content));
        command["jobs"] = SComposeJSONArray(jobs);
        tester->executeWaitVerifyContent(command);

        // Try to create the same child with unique, but pass parent2 instead
        command.clear();
        command.methodLine = "CreateJobs";
        data1["blabla"] = "blabla";
        job1Content["name"] = "createWithParent_child";
        job1Content["unique"] = "true";
        job1Content["data"] = SComposeJSONObject(data1);
        job1Content["parentJobID"] = parentID2;
        jobs.clear();
        jobs.push_back(SComposeJSONObject(job1Content));
        command["jobs"] = SComposeJSONArray(jobs);
        tester->executeWaitVerifyContent(command, "404 Trying to create a child that already exists, but it is tied to a different parent");
    }

} __CreateJobsTest;
