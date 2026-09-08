/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    UpgradeDBTest.cpp
 * Path:    test/clustertest/tests/UpgradeDBTest.cpp
 *
 * INTENT
 *   Cluster test verifying that a schema change made by `upgradeDatabase`
 *   (creating the `dbupgrade` table) replicates to every follower, by
 *   writing a row on the leader and then reading it back locally on every
 *   node without escalation.
 *
 * OBJECTS
 *   UpgradeDBTest        - tpunit fixture; brings up a default cluster.
 *   UpgradeDBTest::test - writes a row on the leader, waits for followers' commit counts to catch
 *                          up, then reads the row back from all three nodes directly.
 *   __UpgradeDBTest      - static instance that registers the fixture with tpunit.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with sibling test files.
 * ─────────────────────────────────────────────────────────────────────*/
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct UpgradeDBTest : tpunit::TestFixture
{
    UpgradeDBTest()
        : tpunit::TestFixture("UpgradeDB",
                              BEFORE_CLASS(UpgradeDBTest::setup),
                              AFTER_CLASS(UpgradeDBTest::teardown),
                              TEST(UpgradeDBTest::test))
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
        // Write one row on the leader. Write commands always run on the leader regardless of
        // which node they're sent to, so this only confirms the table exists on the leader.
        SData insert("Query");
        insert["Query"] = "INSERT INTO dbupgrade VALUES(1, " + SQ("val") + ");";
        tester->getTester(0).executeWaitVerifyContent(insert, "200");

        // Capture the leader's commit count now that the INSERT has landed. Followers
        // replicate asynchronously, so without waiting they might not yet have the row.
        string leaderCommitCount = SParseJSONObject(
            tester->getTester(0).executeWaitVerifyContent(SData("Status"), "200", true)
            )["CommitCount"];
        for (auto i : {1, 2}) {
            ASSERT_TRUE(tester->getTester(i).waitForStatusTerm("CommitCount", leaderCommitCount));
        }

        // Read the same row back on every node, including both followers. Unlike writes,
        // read queries are processed locally on the receiving node without being escalated to
        // the leader. If upgradeDatabase's CREATE TABLE was not replicated to a follower, the
        // SELECT will fail on that node, catching incomplete schema replication.
        for (auto i : {0, 1, 2}) {
            ASSERT_EQUAL(tester->getTester(i).readDB("SELECT value FROM dbupgrade WHERE id = 1;"), "val");
        }
    }
} __UpgradeDBTest;
