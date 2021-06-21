#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct WriteTest : tpunit::TestFixture {
    WriteTest()
        : tpunit::TestFixture("Write",
                              BEFORE_CLASS(WriteTest::setup),
                              TEST(WriteTest::insert),
                              TEST(WriteTest::parallelInsert),
                              TEST(WriteTest::failedDeleteNoWhere),
                              TEST(WriteTest::deleteNoWhereFalse),
                              TEST(WriteTest::deleteNoWhereTrue),
                              TEST(WriteTest::deleteWithWhere),
                              TEST(WriteTest::update),
                              TEST(WriteTest::failedUpdateNoWhere),
                              TEST(WriteTest::failedUpdateNoWhereTrue),
                              TEST(WriteTest::failedUpdateNoWhereFalse),
                              TEST(WriteTest::updateAndInsertWithHttp),
                              TEST(WriteTest::shortHandSyntax),
                              TEST(WriteTest::keywordsAsValue),
                              AFTER_CLASS(WriteTest::tearDown)) { }

    BedrockTester* tester;

    void setup() {
        tester = new BedrockTester({}, {
            "CREATE TABLE foo (bar INTEGER);",
            "CREATE TABLE stuff (id INTEGER PRIMARY KEY, value INTEGER, info TEXT);",
        });
    }

    void tearDown() {
        delete tester;
    }

    void insert() {
        for (int i = 0; i < 50; i++) {
            SData query("Query");
            query["writeConsistency"] = "ASYNC";
            query["query"] = "INSERT INTO foo VALUES ( RANDOM() );";
            tester->executeWaitVerifyContent(query);
        }

        SData query("Query");
        query["query"] = "SELECT COUNT(*) FROM foo;";
        string response = tester->executeWaitVerifyContent(query);
        // Skip the header line.
        string secondLine = response.substr(response.find('\n') + 1);
        int val = SToInt(secondLine);
        ASSERT_EQUAL(val, 50);
    }

    void parallelInsert() {
        vector<SData> requests;
        int numCommands = 50;
        for (int i = 0; i < numCommands; i++) {
            SData query("Query");
            query["writeConsistency"] = "ASYNC";
            query["debugID"] = "parallelCommand#" + to_string(i);
            query["query"] = "INSERT INTO stuff VALUES ( NULL, " + SQ(i) + ", NULL );";
            requests.push_back(query);
        }
        auto results = tester->executeWaitMultipleData(requests);

        int success = 0;
        int failure = 0;

        for (auto& row : results) {
            if (SToInt(row.methodLine) == 200) {
                success++;
            } else {
                failure++;
            }
        }

        ASSERT_EQUAL(success, numCommands);

        // Verify there's actually data there.
        SData query("Query");
        query["query"] = "SELECT COUNT(*) FROM stuff;";
        string response = tester->executeWaitVerifyContent(query);
        // Skip the header line.
        string secondLine = response.substr(response.find('\n') + 1);
        int val = SToInt(secondLine);
        ASSERT_EQUAL(val, numCommands);
    }

    void failedDeleteNoWhere() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "DELETE FROM foo;";
        tester->executeWaitVerifyContent(query, "502 Query aborted");
    }

    void deleteNoWhereFalse() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "DELETE FROM foo;";
        query["nowhere"] = "false";
        tester->executeWaitVerifyContent(query, "502 Query aborted");
    }

    void deleteNoWhereTrue() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "DELETE FROM foo;";
        query["nowhere"] = "true";
        tester->executeWaitVerifyContent(query);
    }

    void deleteWithWhere() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "INSERT INTO foo VALUES ( 666 );";
        tester->executeWaitVerifyContent(query);

        query["query"] = "DELETE FROM foo WHERE bar = 666;";
        tester->executeWaitVerifyContent(query);
    }

    void update() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "INSERT INTO foo VALUES ( 666 );";
        tester->executeWaitVerifyContent(query);

        query["query"] = "UPDATE foo SET bar = 777 WHERE bar = 666;";
        tester->executeWaitVerifyContent(query);
    }

    void failedUpdateNoWhere() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "UPDATE foo SET bar = 0;";
        tester->executeWaitVerifyContent(query, "502 Query aborted");
    }

    void failedUpdateNoWhereFalse() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "UPDATE foo SET bar = 0;";
        query["nowhere"] = "false";
        tester->executeWaitVerifyContent(query, "502 Query aborted");
    }

    void failedUpdateNoWhereTrue() {
        SData query("Query");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "UPDATE foo SET bar = 0;";
        query["nowhere"] = "true";
        tester->executeWaitVerifyContent(query);
    }

    void updateAndInsertWithHttp() {
        SData query("Query / HTTP/1.1");
        query["writeConsistency"] = "ASYNC";
        query["query"] = "INSERT INTO foo VALUES ( 666 );";
        tester->executeWaitVerifyContent(query);

        query["query"] = "UPDATE foo SET bar = 777 WHERE bar = 666;";
        tester->executeWaitVerifyContent(query);
    }

    void shortHandSyntax() {
        SData query("query: UPDATE stuff SET value = 2 WHERE id = 1;");
        tester->executeWaitVerifyContent(query);

        SData query2("Query: UPDATE stuff SET value = 3 WHERE id = 2;");
        tester->executeWaitVerifyContent(query2);
    }

    void keywordsAsValue() {
        SData query("query: INSERT INTO stuff VALUES ( NULL, 11, 'Please update the test' );");
        tester->executeWaitVerifyContent(query);

        SData query2("query: INSERT INTO stuff VALUES ( NULL, 12, 'Do not delete the test' );");
        tester->executeWaitVerifyContent(query2);

        // This is a false test case, This query shouldn't get executed. This is currently a limitation of our parsing
        // As "nowhere" parameter is not provided, this query should get aborted and prevent all rows from updating
        // Change the expected result to "502 Query aborted" once https://github.com/Expensify/Expensify/issues/165207 is solved
        SData query3("query: UPDATE stuff SET info = 'This is not a where clause';");
        tester->executeWaitVerifyContent(query3);
    }

} __WriteTest;
