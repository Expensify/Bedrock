#include "../BedrockClusterTester.h"

struct JobIDTest : tpunit::TestFixture {
    JobIDTest()
        : tpunit::TestFixture("jobID",
                              BEFORE_CLASS(JobIDTest::setup),
                              AFTER_CLASS(JobIDTest::teardown),
                              TEST(JobIDTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void setup () {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown () {
        delete tester;
    }

    void test()
    {
        BedrockTester* master = tester->getBedrockTester(0);
        BedrockTester* slave = tester->getBedrockTester(1);

        // Create a job in master
        SData createCmd("CreateJob");
        createCmd["name"] = "TestJob";
        STable response = master->executeWaitVerifyContentTable(createCmd);
        const int jobID = SToInt(response["jobID"]);

        // Stop master
        tester->stopNode(0);

        int count = 0;
        bool success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = slave->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }

            // Give it another second...
            sleep(1);
        }

        // make sure it actually succeeded.
        ASSERT_TRUE(success);

        // Create a job in the slave and check the ID returned is the next one
        response = slave->executeWaitVerifyContentTable(createCmd, "200");
        ASSERT_EQUAL(jobID + 1, SToInt(response["jobID"]));

        // Restart master
        tester->startNode(0);

        count = 0;
        success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = master->executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["state"] == "MASTERING") {
                success = true;
                break;
            }

            // Give it another second...
            sleep(1);
        }

        // Create a new job in master and check the ID is the next one
        response = master->executeWaitVerifyContentTable(createCmd);
        ASSERT_EQUAL(jobID + 2, SToInt(response["jobID"]));

        // Get the 3 jobs to leave the db clean
        SData getCmd("GetJobs");
        getCmd["name"] = "*";
        getCmd["numResults"] = 3;
        slave->executeWaitVerifyContentTable(getCmd, "200");
    }

} __JobIDTest;
