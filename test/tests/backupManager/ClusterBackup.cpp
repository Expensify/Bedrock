#include <test/clustertest/BedrockClusterTester.h>

struct ClusterBackupTest : tpunit::TestFixture
{
    ClusterBackupTest() : tpunit::TestFixture(BEFORE_CLASS(ClusterBackupTest::setupClass),
                                              TEST(ClusterBackupTest::testFullClusterUp),
                                              TEST(ClusterBackupTest::testHalfClusterStateCheck),
                                              AFTER_CLASS(ClusterBackupTest::tearDownClass))
    {
        NAME(ClusterBackup);
    }

    BedrockClusterTester* testCluster;
    BedrockTester* uploadTester;
    BedrockTester* downloadTester;
    string manifestFileName;
    SData command;

    void setupClass()
    {
        command.methodLine = "BeginExpensifyBackup";
        command["key"] = "a77477f1609c0e184427f8b39a02eb1c8c1fa3d509b131ba101d7b4019eb81a1";
        command["threads"] = "4";
        command["chunkSize"] = "1048576";

        // Load our SQL file and run the queries from inside of it
        string dbSql = SFileLoad("data/db.sql");
        const list<string>& queries = SParseList(dbSql, ';');

        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }

        // Start a cluster with one permafollower
        testCluster = new BedrockClusterTester(ClusterSize::SIX_NODE_CLUSTER,
                                                queries, 0,
                                               {{"-plugins", cwd + "/../expensifyBackupManager.so"s},
                                                {"-backupKeyFile", "data/key.key"}},
                                               {}, "../expensifyBackupManager.so");
    }

    void tearDownClass()
    {
        delete testCluster;
    }

    void testFullClusterUp()
    {
        // Pick a follower
        BedrockTester* uploadTester = testCluster->getBedrockTester(2);

        // Do the backup
        runSuccessfulBackup(uploadTester);
    }

    void testHalfClusterStateCheck()
    {
        // Stop two full peers so 3 full peers and 1 permaslave remain.
        testCluster->stopNode(1);
        testCluster->stopNode(2);

        // Pick a test that's still online.
        BedrockTester* uploadTester = testCluster->getBedrockTester(3);

        // Try to run a backup, it should error.
        auto results = uploadTester->executeWaitMultipleData({command}, 1, true);
        ASSERT_EQUAL(results[0].methodLine, "501 Unable to backup, not enough peers");

        // Stop the permafollower, make sure it still errors.
        testCluster->stopNode(5);
        results = uploadTester->executeWaitMultipleData({command}, 1, true);
        ASSERT_EQUAL(results[0].methodLine, "501 Unable to backup, not enough peers");

        // Start the permafollower back up, and run a backup on it. This should always work.
        testCluster->startNode(5);
        testCluster->getBedrockTester(5)->waitForState("FOLLOWING");
        runSuccessfulBackup(testCluster->getBedrockTester(5));

        // Turn 1 peer back on, wait for it to be a follower, the backup should work now.
        testCluster->startNode(2);
        testCluster->getBedrockTester(2)->waitForState("FOLLOWING");
        runSuccessfulBackup(uploadTester);
    }

    void runSuccessfulBackup(BedrockTester* node)
    {
        auto results = node->executeWaitMultipleData({command}, 1, true);
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        manifestFileName = results[0].nameValueMap["manifestFileName"];
        ASSERT_TRUE(!manifestFileName.empty());

        // Wait up to 50s for it to come back up.
        int count = 0;
        int secondsToWait = 50;
        while (count++ < secondsToWait) {
            SData cmd("Status");
            try {
                string response = node->executeWaitVerifyContent(cmd);
                STable json = SParseJSONObject(response);
                if (json["state"] == "FOLLOWING") {
                    break;
                }
            } catch (const SException& e) {
                if (count == secondsToWait) {
                    STHROW("Never finished uploading.");
                }
            }

            // Give it another second...
            sleep(1);
        }
    }
} __ClusterBackupTest;
