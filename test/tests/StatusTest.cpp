/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    StatusTest.cpp
 * Path:    test/tests/StatusTest.cpp
 *
 * INTENT
 *   Smoke test confirming the "Status" command's response includes the
 *   expected diagnostic fields (plugins, outstandingFramesToCheckpoint,
 *   freelistCount, pageCount).
 *
 * OBJECTS
 *   StatusTest  - tpunit::TestFixture; single `test` checks the Status
 *                 response for the presence of those field names.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits: alongside the other single-command tests in test/tests.
 *
 * NAMING QUALITY
 *   Consistent with sibling fixtures.
 * ─────────────────────────────────────────────────────────────────────*/

#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct StatusTest : tpunit::TestFixture
{
    StatusTest()
        : tpunit::TestFixture("Status", TEST(StatusTest::test))
    {
    }

    void test()
    {
        BedrockTester tester;
        SData status("Status");
        string response = tester.executeWaitMultipleData({status})[0].content;
        ASSERT_TRUE(SContains(response, "plugins"));
        ASSERT_TRUE(SContains(response, "outstandingFramesToCheckpoint"));
        ASSERT_TRUE(SContains(response, "freelistCount"));
        ASSERT_TRUE(SContains(response, "pageCount"));
    }
} __StatusTest;
