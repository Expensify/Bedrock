/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    ReadTest.cpp
 * Path:    test/tests/ReadTest.cpp
 *
 * INTENT
 *   Smoke tests for bedrock's "Query" command handling a plain read-only
 *   SELECT: with and without an HTTP method line, and confirming a query
 *   missing its trailing semicolon is rejected.
 *
 * OBJECTS
 *   ReadTest  - tpunit::TestFixture; simpleRead/simpleReadWithHttp run
 *               "SELECT 1;" through a fresh BedrockTester and check the
 *               returned value; readNoSemicolon checks the 502 rejection
 *               of a malformed query.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   "ReadTest" is a generic name for what is specifically a Query-command
 *   test; fits its location among other single-command tests in
 *   test/tests but the name gives little hint that it's about Query.
 *
 * NAMING QUALITY
 *   Consistent with sibling fixtures.
 * ─────────────────────────────────────────────────────────────────────*/

#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct ReadTest : tpunit::TestFixture
{
    ReadTest()
        : tpunit::TestFixture("Read",
                              BEFORE_CLASS(ReadTest::setup),
                              TEST(ReadTest::simpleRead),
                              TEST(ReadTest::simpleReadWithHttp),
                              TEST(ReadTest::readNoSemicolon),
                              AFTER_CLASS(ReadTest::tearDown))
    {
    }

    BedrockTester* tester;

    void setup()
    {
        tester = new BedrockTester();
    }

    void tearDown()
    {
        delete tester;
    }

    void simpleRead()
    {
        SData status("Query");
        status["query"] = "SELECT 1;";
        string response = tester->executeWaitVerifyContent(status);
        int val = SToInt(response);
        ASSERT_EQUAL(val, 1);
    }

    void simpleReadWithHttp()
    {
        SData status("Query / HTTP/1.1");
        status["query"] = "SELECT 1;";
        string response = tester->executeWaitVerifyContent(status);
        int val = SToInt(response);
        ASSERT_EQUAL(val, 1);
    }

    void readNoSemicolon()
    {
        SData status("Query");
        status["query"] = "SELECT 1";
        tester->executeWaitVerifyContent(status, "502");
    }
} __ReadTest;
