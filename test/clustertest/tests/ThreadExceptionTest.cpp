/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    ThreadExceptionTest.cpp
 * Path:    test/clustertest/tests/ThreadExceptionTest.cpp
 *
 * INTENT
 *   Cluster test verifying that the "ThreadException" test command behaves
 *   as a normal 200 OK by default, and reports "500 THREAD THREW" when told
 *   to rethrow, confirming an exception on a worker thread surfaces as a
 *   proper error response rather than crashing or hanging the server.
 *
 * OBJECTS
 *   ThreadExceptionTest        - tpunit fixture, single free-standing test.
 *   ThreadExceptionTest::test - sends the command plain, then again with `rethrow=true`.
 *   __ThreadExceptionTest      - static instance that registers the fixture with tpunit.
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

struct ThreadExceptionTest : tpunit::TestFixture
{
    ThreadExceptionTest()
        : tpunit::TestFixture("ThreadException", TEST(ThreadExceptionTest::test))
    {
    }

    void test()
    {
        BedrockClusterTester tester;

        SData command("ThreadException");
        SData result = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_EQUAL(result.methodLine, "200 OK");

        command["rethrow"] = "true";
        result = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_EQUAL(result.methodLine, "500 THREAD THREW");
    }
} __ThreadExceptionTest;
