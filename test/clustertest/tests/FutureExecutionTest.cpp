/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    FutureExecutionTest.cpp
 * Path:    test/clustertest/tests/FutureExecutionTest.cpp
 *
 * INTENT
 *   Cluster test for leader-only scheduled ("future") command execution:
 *   a write scheduled a few seconds out isn't applied early but does land
 *   once its time passes, and a command depending on a commit count that
 *   will never arrive times out ("555 Timeout") rather than hanging.
 *
 * OBJECTS
 *   FutureExecutionTest                     - tpunit fixture
 *   FutureExecutionTest::setup/teardown     - own the BedrockClusterTester
 *   FutureExecutionTest::FutureExecution    - schedules an insert a few
 *                        seconds in the future, confirms it's absent
 *                        immediately after and present once the delay passes
 *   FutureExecutionTest::FutureExecutionTimeout - a query with a
 *                        commitCount that will never be reached and a
 *                        100ms timeout returns "555 Timeout"
 *
 * OUT OF PLACE
 *   [CANDIDATE] #include <fstream> is unused; no stream type from it appears
 *   anywhere in the file.
 *
 * NAME/LOCATION FIT
 *   Fits: a scheduled-execution test alongside its peers.
 *
 * NAMING QUALITY
 *   FutureExecution/FutureExecutionTimeout are PascalCase, unlike every
 *   other TEST() method in this file's siblings (testXxx-style camelCase),
 *   and echo the fixture's own name closely enough to read as constructors
 *   at a glance.
 * ─────────────────────────────────────────────────────────────────────*/
#include <fstream>

#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct FutureExecutionTest : tpunit::TestFixture
{
    FutureExecutionTest()
        : tpunit::TestFixture("FutureExecution",
                              BEFORE_CLASS(FutureExecutionTest::setup),
                              AFTER_CLASS(FutureExecutionTest::teardown),
                              TEST(FutureExecutionTest::FutureExecution),
                              TEST(FutureExecutionTest::FutureExecutionTimeout))
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

    void FutureExecution()
    {
        // We only care about leader because future execution only works on leader.
        BedrockTester& brtester = tester->getTester(0);

        // Let's run a command in the future.
        SData query("Query");

        // Three seconds from now.
        query["commandExecuteTime"] = to_string(STimeNow() + 3000000);
        query["Query"] = "INSERT INTO test VALUES(" + SQ(50011) + ", " + SQ("sent_by_leader") + ");";
        string result = brtester.executeWaitVerifyContent(query, "202");

        // Ok, Now let's wait a second
        sleep(1);

        // And make it still hasn't been inserted.
        query.clear();
        query.methodLine = "Query";
        query["Query"] = "SELECT * FROM test WHERE id = 50011;";
        result = brtester.executeWaitVerifyContent(query);
        ASSERT_FALSE(SContains(result, "50011"));

        // Then sleep three more seconds, it *should* be there now.
        sleep(3);

        // And now it should be there, but we'll give it a couple tries.
        int retries = 3;
        bool success = false;
        while (retries) {
            result = brtester.executeWaitVerifyContent(query);
            if (SContains(result, "50011")) {
                success = true;
                break;
            } else {
                sleep(1);
                retries--;
            }
        }
        ASSERT_TRUE(success);
    }

    void FutureExecutionTimeout()
    {
        // We only care about leader because future execution only works on leader.
        BedrockTester& brtester = tester->getTester(0);

        // Let's make a query that depends on a commit that will never happen.
        SData query("Query");
        query["commitCount"] = to_string(UINT64_MAX);

        // But only allow it 0.1s to complete.
        query["timeout"] = "100"; // 100ms.

        // And, there's a query to run, too, I guess.
        query["Query"] = "SELECT 1;";
        brtester.executeWaitVerifyContent(query, "555 Timeout");
    }
} __FutureExecutionTest;
