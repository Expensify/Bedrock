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
        SData cmd("CreateJob");
        cmd["name"] = "TestJob";
        STable response = master->executeWaitVerifyContentTable(cmd);
        const int jobID = SToInt(response["jobID"]);

        // Stop master
        tester->stopNode(0);
        response = slave->executeWaitVerifyContentTable(cmd, "200");
        ASSERT_EQUAL(jobID + 1, SToInt(response["jobID"]));
    }

} __k_JobIDTest;
