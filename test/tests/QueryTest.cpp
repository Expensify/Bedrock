#include "libstuff/libstuff.h"
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct QueryTest : tpunit::TestFixture
{
    QueryTest()
        : tpunit::TestFixture("Query",
                              BEFORE_CLASS(QueryTest::setup),
                              TEST(QueryTest::testMissing),
                              TEST(QueryTest::testNoSemicolon),
                              TEST(QueryTest::testBad),
                              TEST(QueryTest::testOK),
                              TEST(QueryTest::testWrite),
                              TEST(QueryTest::testWriteInSecondStatement),
                              TEST(QueryTest::testNoWhere),
                              TEST(QueryTest::testPercentile),
                              AFTER_CLASS(QueryTest::tearDown))
    {
    }

    BedrockTester* tester;

    void setup()
    {
        tester = new BedrockTester({}, {
            "CREATE TABLE queryTest (key INTEGER, value TEXT);",
        });
    }

    void tearDown()
    {
        delete tester;
    }

    void testMissing()
    {
        SData query("Query");
        query["ReadDBFlags"] = "-json";
        query["query"] = "";
        tester->executeWaitVerifyContent(query, "402 Missing query");
    }

    void testNoSemicolon()
    {
        SData query("Query");
        query["query"] = "SELECT MAX(key) FROM queryTest";
        tester->executeWaitVerifyContent(query, "502 Query Missing Semicolon");
    }

    void testBad()
    {
        SData query("Query");
        query["query"] = "this is a garbage query;";
        tester->executeWaitVerifyContent(query, "402 Bad query");
    }

    void testOK()
    {
        SData query("Query");
        query["query"] = "SELECT 1;";
        tester->executeWaitVerifyContent(query, "200 OK");
    }

    void testWrite()
    {
        SData query("Query");
        query["ReadDBFlags"] = "-json";
        query["query"] = "INSERT INTO queryTest VALUES(1, 'first value');";
        tester->executeWaitVerifyContent(query);

        query["query"] = "SELECT value FROM queryTest WHERE key = 1;";
        string resultJSON = tester->executeWaitMultipleData({query})[0].content;

        // Parse the first item in the first row of results and check that.
        ASSERT_EQUAL(SParseJSONObject(SParseJSONArray(resultJSON).front())["value"], "first value");
    }

    void testWriteInSecondStatement()
    {
        SData query("Query");
        query["ReadDBFlags"] = "-json";
        query["query"] = "SELECT 1; INSERT INTO queryTest VALUES(2, 'second value');";
        tester->executeWaitVerifyContent(query);

        query["query"] = "SELECT value FROM queryTest WHERE key = 2;";
        string resultJSON = tester->executeWaitMultipleData({query})[0].content;

        // Parse the first item in the first row of results and check that.
        ASSERT_EQUAL(SParseJSONObject(SParseJSONArray(resultJSON).front())["value"], "second value");
    }

    void testNoWhere()
    {
        SData query("Query");
        query["query"] = "DELETE FROM queryTest;";
        tester->executeWaitVerifyContent(query, "502 Query aborted");
    }

    // Verify the SQLITE_ENABLE_PERCENTILE functions (median, percentile,
    // percentile_cont, percentile_disc) are compiled in and registered.
    void testPercentile()
    {
        // The values 10, 20, 30, 40, 50 are supplied inline so this test doesn't depend
        // on any rows left behind in queryTest by earlier tests.
        const string values = "(SELECT 10 AS v UNION ALL SELECT 20 UNION ALL SELECT 30 UNION ALL SELECT 40 UNION ALL SELECT 50)";

        auto scalarResult = [&](const string& expression) -> double {
            SData query("Query");
            query["ReadDBFlags"] = "-json";
            query["query"] = "SELECT " + expression + " AS result FROM " + values + ";";
            string resultJSON = tester->executeWaitMultipleData({query})[0].content;
            return stod(SParseJSONObject(SParseJSONArray(resultJSON).front())["result"]);
        };

        // median(v) == percentile(v, 50) == the middle value.
        ASSERT_EQUAL(scalarResult("median(v)"), 30.0);

        // percentile(v, P) interpolates at P*(N-1)/100; with N=5, P=25 lands exactly on the second value.
        ASSERT_EQUAL(scalarResult("percentile(v, 25)"), 20.0);
        ASSERT_EQUAL(scalarResult("percentile(v, 0)"), 10.0);
        ASSERT_EQUAL(scalarResult("percentile(v, 100)"), 50.0);

        // percentile_cont(v, F) is percentile(v, F*100).
        ASSERT_EQUAL(scalarResult("percentile_cont(v, 0.25)"), 20.0);

        // percentile_disc(v, F) returns one of the input values rather than interpolating.
        ASSERT_EQUAL(scalarResult("percentile_disc(v, 0.5)"), 30.0);
    }
} __QueryTest;
