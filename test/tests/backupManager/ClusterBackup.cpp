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
    char cwd[1024];

    void setupClass()
    {
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }

        command.methodLine = "BeginBackup";
        command["key"] = "a77477f1609c0e184427f8b39a02eb1c8c1fa3d509b131ba101d7b4019eb81a1";
        command["threads"] = "4";
        command["chunkSize"] = "1048576";

        // Load our SQL file and run the queries from inside of it
        string dbSql = SFileLoad(string(cwd) + "/tests/backupManager/data/db.sql");
        const list<string>& queries = SParseList(dbSql, ';');

        // Start a cluster with one permafollower
        testCluster = new ClusterTester<BedrockTester>(ClusterSize::SIX_NODE_CLUSTER, queries, 0,
                                        {{"-backupKeyFile", string(cwd) + "/tests/backupManager/data/key.key"}}, {},
                                        "BackupManager");
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
        testCluster->getBedrockTester(5)->waitForStates({"SLAVING","FOLLOWING"});
        runSuccessfulBackup(testCluster->getBedrockTester(5));

        // Turn 1 peer back on, wait for it to be a follower, the backup should work now.
        testCluster->startNode(2);
        testCluster->getBedrockTester(2)->waitForStates({"SLAVING", "FOLLOWING"});
        runSuccessfulBackup(uploadTester);
    }

    void runSuccessfulBackup(BedrockTester* node)
    {
        auto results = node->executeWaitMultipleData({command}, 1, true);
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        manifestFileName = results[0].nameValueMap["manifestFileName"];
        ASSERT_TRUE(!manifestFileName.empty());

        // Wait up to 50s for it to come back up.
        if(!node->waitForStates({"SLAVING", "FOLLOWING"}, 50'000'000)) {
            STHROW("Never finished uploading.");
        }
        
    }
} __ClusterBackupTest;
