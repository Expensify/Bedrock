#include "../BedrockClusterTester.h"

struct k_JobIDTest : tpunit::TestFixture {
    k_JobIDTest()
        : tpunit::TestFixture("k_jobID",
                              TEST(k_JobIDTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void test()
    {
        tester = BedrockClusterTester::testers.front();
        BedrockTester* master = tester->getBedrockTester(0);
        BedrockTester* slave = tester->getBedrockTester(1);

        // Create a job in master
        SData createCmd("CreateJob");
        createCmd["name"] = "TestJob";
        STable response = master->executeWaitVerifyContentTable(createCmd);
        const int jobID = SToInt(response["jobID"]);

        // Stop master
        tester->stopNode(0);

        // Create a job in the slave and check the ID returned is the next one
        response = slave->executeWaitVerifyContentTable(createCmd, "200");
        ASSERT_EQUAL(jobID + 1, SToInt(response["jobID"]));

        // Restart master
        tester->startNode(0);

        // Create a new job in master and check the ID is the next one
        response = master->executeWaitVerifyContentTable(createCmd);
        ASSERT_EQUAL(jobID + 2, SToInt(response["jobID"]));

        // Get the 3 jobs to leave the db clean
        SData getCmd("GetJobs");
        getCmd["name"] = "*";
        getCmd["numResults"] = 3;
        slave->executeWaitVerifyContentTable(getCmd, "200");
    }

} __k_JobIDTest;
