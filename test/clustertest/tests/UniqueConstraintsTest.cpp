/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    UniqueConstraintsTest.cpp
 * Path:    test/clustertest/tests/UniqueConstraintsTest.cpp
 *
 * INTENT
 *   Cluster test verifying that inserting the same row twice succeeds the
 *   first time (200) and is rejected the second time (400) by SQLite's
 *   unique-constraint check, exercised through the leader.
 *
 * OBJECTS
 *   UniqueConstraintsTest        - tpunit fixture, single free-standing test.
 *   UniqueConstraintsTest::test - sends the same INSERT command twice and checks each response code.
 *   __UniqueConstraintsTest      - static instance that registers the fixture with tpunit.
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

struct UniqueConstraintsTest : tpunit::TestFixture
{
    UniqueConstraintsTest()
        : tpunit::TestFixture("UniqueConstraints", TEST(UniqueConstraintsTest::test))
    {
    }

    void test()
    {
        BedrockClusterTester tester;
        SData command("Query");
        command["Query"] = "INSERT INTO test VALUES(" + SQ(1) + ", " + SQ("val") + ");";
        auto result1 = tester.getTester(0).executeWaitMultipleData({command})[0];
        auto result2 = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_TRUE(SStartsWith(result1.methodLine, "200"));
        ASSERT_TRUE(SStartsWith(result2.methodLine, "400"));
    }
} __UniqueConstraintsTest;
