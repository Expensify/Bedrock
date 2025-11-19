#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct QueryJobTest : tpunit::TestFixture
{
    QueryJobTest()
        : tpunit::TestFixture("QueryJob",
                              BEFORE_CLASS(QueryJobTest::setupClass),
                              TEST(QueryJobTest::queryJob),
                              AFTER(QueryJobTest::tearDown),
                              AFTER_CLASS(QueryJobTest::tearDownClass))
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

    void queryJob()
    {
        // Create the job
        SData command("CreateJob");
        string jobName = "job";
        command["name"] = jobName;
        command["firstRun"] = "2042-04-02 00:42:42";
        command["repeat"] = "FINISHED, +1 DAY";
        command["data"] = "{\"something\":1}";
        command["priority"] = "1000";
        STable response = tester->executeWaitVerifyContentTable(command);
        string jobID = response["jobID"];
        ASSERT_GREATER_THAN(stol(jobID), 0);

        command.clear();
        command.methodLine = "QueryJob";
        command["jobID"] = jobID;
        response = tester->executeWaitVerifyContentTable(command);
        ASSERT_EQUAL(response.size(), 10);
        ASSERT_EQUAL(response["jobID"], jobID);
        ASSERT_EQUAL(response["name"], jobName);
        ASSERT_EQUAL(response["nextRun"], "2042-04-02 00:42:42");
        ASSERT_EQUAL(response["repeat"], "FINISHED, +1 DAY");
        ASSERT_EQUAL(response["data"], "{\"something\":1}");
        ASSERT_EQUAL(response["priority"], "1000");
        SASSERT(!response["created"].empty());
    }
} __QueryJobTest;
