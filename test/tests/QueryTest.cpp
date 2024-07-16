#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct QueryTest : tpunit::TestFixture {
    QueryTest() : tpunit::TestFixture("Query") {
        registerTests(BEFORE_CLASS(QueryTest::setup),
                      TEST(QueryTest::testMissing),
                      TEST(QueryTest::testNoSemicolon),
                      TEST(QueryTest::testBad),
                      TEST(QueryTest::testOK),
                      TEST(QueryTest::testWrite),
                      TEST(QueryTest::testWriteInSecondStatement),
                      TEST(QueryTest::testNoWhere),
                      AFTER_CLASS(QueryTest::tearDown));
    }

    BedrockTester* tester;

    void setup() {
        tester = new BedrockTester({}, {
            "CREATE TABLE queryTest (key INTEGER, value TEXT);",
        });
    }

    void tearDown() {
        delete tester;
    }

    void testMissing() {
        SData query("Query");
        query["Format"] = "json";
        query["query"] = "";
        tester->executeWaitVerifyContent(query, "402 Missing query");
    }

    void testNoSemicolon() {
        SData query("Query");
        query["Format"] = "json";
        query["query"] = "SELECT MAX(key) FROM queryTest";
        tester->executeWaitVerifyContent(query, "502 Query Missing Semicolon");
    }

    void testBad() {
        SData query("Query");
        query["Format"] = "json";
        query["query"] = "this is a garbage query;";
        tester->executeWaitVerifyContent(query, "402 Bad query");
    }

    void testOK() {
        SData query("Query");
        query["Format"] = "json";
        query["query"] = "SELECT 1;";
        tester->executeWaitVerifyContent(query, "200 OK");
    }

    void testWrite() {
        SData query("Query");
        query["Format"] = "json";
        query["query"] = "INSERT INTO queryTest VALUES(1, 'first value');";
        tester->executeWaitVerifyContent(query);

        query["query"] = "SELECT value FROM queryTest WHERE key = 1;";
        STable result = tester->executeWaitVerifyContentTable(query);

        // Parse the first item in the first row of results and check that.
        ASSERT_EQUAL(SParseJSONArray(SParseJSONArray(result["rows"]).front()).front(), "first value");
    }

    void testWriteInSecondStatement() {
        SData query("Query");
        query["Format"] = "json";
        query["query"] = "SELECT 1; INSERT INTO queryTest VALUES(2, 'second value');";
        tester->executeWaitVerifyContent(query);

        query["query"] = "SELECT value FROM queryTest WHERE key = 2;";
        STable result = tester->executeWaitVerifyContentTable(query);

        // Parse the first item in the first row of results and check that.
        ASSERT_EQUAL(SParseJSONArray(SParseJSONArray(result["rows"]).front()).front(), "second value");
    }

    void testNoWhere() {
        SData query("Query");
        query["Format"] = "json";
        query["query"] = "DELETE FROM queryTest;";
        tester->executeWaitVerifyContent(query, "502 Query aborted");
    }
} __QueryTest;
