#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct JobIDTest : tpunit::TestFixture
{
    JobIDTest()
        : tpunit::TestFixture("JobID",
                              BEFORE_CLASS(JobIDTest::setup),
                              AFTER_CLASS(JobIDTest::teardown),
                              TEST(JobIDTest::test)
        )
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    void test()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);

        // Create a job in leader
        SData createCmd("CreateJob");
        createCmd["name"] = "TestJob";
        STable response = leader.executeWaitVerifyContentTable(createCmd);

        // Restart follower. This is a regression test, before we only re-initialized the lastID if it was !=0 which made
        // these tests pass (because the first ID is 0) but fail in the real life. So here we make sure that when a follower
        // becomes leader, it gets the correct ID and the inserts do not fail the unique constrain due to repeated ID.
        tester->stopNode(1);
        tester->startNode(1);

        // Stop leader
        tester->stopNode(0);

        int count = 0;
        bool success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = follower.executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["isLeader"] == "true") {
                success = true;
                break;
            }

            // Give it another second...
            sleep(1);
        }

        // Make sure it actually succeeded.
        ASSERT_TRUE(success);

        // Create a job in the follower
        response = follower.executeWaitVerifyContentTable(createCmd, "200");

        // Restart leader
        tester->startNode(0);

        count = 0;
        success = false;
        while (count++ < 50) {
            SData cmd("Status");
            string response = leader.executeWaitVerifyContent(cmd);
            STable json = SParseJSONObject(response);
            if (json["isLeader"] == "true") {
                success = true;
                break;
            }

            // Give it another second...
            sleep(1);
        }

        // Make sure it also succeeded.
        ASSERT_TRUE(success);

        // Create a new job in leader.
        response = leader.executeWaitVerifyContentTable(createCmd);

        // Get the 3 jobs to leave the db clean
        SData getCmd("GetJobs");
        getCmd["name"] = "*";
        getCmd["numResults"] = 3;
        follower.executeWaitVerifyContentTable(getCmd, "200");
    }
} __JobIDTest;
