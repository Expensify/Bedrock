#include "../BedrockClusterTester.h"

struct JobIDTest : tpunit::TestFixture {
    JobIDTest()
        : tpunit::TestFixture("JobID",
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

        // Restart slave. This is a regression test, before we only re-initialized the lastID if it was !=0 which made
        // these tests pass (because the first ID is 0) but fail in the real life. So here we make sure that when a slave
        // becomes master, it gets the correct ID and the inserts do not fail the unique constrain due to repeated ID.
        tester->stopNode(1);
        tester->startNode(1);

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

        // Create a job in the slave
        response = slave->executeWaitVerifyContentTable(createCmd, "200");

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

        // Create a new job in master.
        response = master->executeWaitVerifyContentTable(createCmd);

        // Get the 3 jobs to leave the db clean
        SData getCmd("GetJobs");
        getCmd["name"] = "*";
        getCmd["numResults"] = 3;
        slave->executeWaitVerifyContentTable(getCmd, "200");
    }

} __JobIDTest;
