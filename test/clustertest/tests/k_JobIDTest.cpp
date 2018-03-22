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

        // Create a job
        SData createCmd("CreateJob");
        createCmd["name"] = "TestJob";
        STable response = master->executeWaitVerifyContentTable(createCmd);
        const int jobID = SToInt(response["jobID"]);

        // Stop master
        tester->stopNode(0);
        response = slave->executeWaitVerifyContentTable(createCmd, "200");
        ASSERT_EQUAL(jobID + 1, SToInt(response["jobID"]));

        // Get the 2 jobs to leave the db clean.
        SData getCmd("GetJob");
        getCmd["name"] = "TestJob";
        getCmd["name"] = "*";
        slave->executeWaitVerifyContentTable(getCmd, "200");
        slave->executeWaitVerifyContentTable(getCmd, "200");
    }

} __k_JobIDTest;
