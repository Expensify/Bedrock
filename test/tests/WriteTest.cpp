#include <test/lib/BedrockTester.h>

struct WriteTest : tpunit::TestFixture {
    WriteTest()
        : tpunit::TestFixture("Write",
                              BEFORE_CLASS(WriteTest::setup),
                              TEST(WriteTest::insert),
                              TEST(WriteTest::parallelInsert),
                              TEST(WriteTest::failedInsertNoSemiColon),
                              TEST(WriteTest::failedDeleteNoWhere),
                              TEST(WriteTest::deleteNoWhereFalse),
                              TEST(WriteTest::deleteNoWhereTrue),
                              TEST(WriteTest::deleteWithWhere),
                              TEST(WriteTest::update),
                              TEST(WriteTest::failedUpdateNoWhere),
                              TEST(WriteTest::failedUpdateNoWhereTrue),
                              TEST(WriteTest::failedUpdateNoWhereFalse),
                              TEST(WriteTest::updateAndInsertWithHttp),
                              AFTER_CLASS(WriteTest::tearDown)) { }

    BedrockTester* tester;

    void setup() {
        tester = new BedrockTester({}, {
            "CREATE TABLE foo (bar INTEGER);",
            "CREATE TABLE stuff (id INTEGER PRIMARY KEY, value INTEGER);",
        });
    }

    void tearDown() {
        delete tester;
    }

    void insert() {
        for (int i = 0; i < 50; i++) {
            SData status("Query");
            status["writeConsistency"] = "ASYNC";
            status["query"] = "INSERT INTO foo VALUES ( RANDOM() );";
            tester->executeWaitVerifyContent(status);
        }

        SData status("Query");
        status["query"] = "SELECT COUNT(*) FROM foo;";
        string response = tester->executeWaitVerifyContent(status);
        // Skip the header line.
        string secondLine = response.substr(response.find('\n') + 1);
        int val = SToInt(secondLine);
        ASSERT_EQUAL(val, 50);
    }

    void parallelInsert() {
        vector<SData> requests;
        int numCommands = 50;
        cout << "Testing with " << numCommands << " commands." << endl;
        for (int i = 0; i < numCommands; i++) {
            SData query("Query");
            query["writeConsistency"] = "ASYNC";
            query["debugID"] = "parallelCommand#" + to_string(i);
            query["query"] = "INSERT INTO stuff VALUES ( NULL, " + SQ(i) + " );";
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
        SData status("Query");
        status["query"] = "SELECT COUNT(*) FROM stuff;";
        string response = tester->executeWaitVerifyContent(status);
        // Skip the header line.
        string secondLine = response.substr(response.find('\n') + 1);
        int val = SToInt(secondLine);
        ASSERT_EQUAL(val, numCommands);
    }

    void failedInsertNoSemiColon() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "INSERT INTO foo VALUES ( RANDOM() )";
        tester->executeWaitVerifyContent(status, "502 Query aborted");
    }

    void failedDeleteNoWhere() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "DELETE FROM foo;";
        tester->executeWaitVerifyContent(status, "502 Query aborted");
    }

    void deleteNoWhereFalse() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "DELETE FROM foo;";
        status["nowhere"] = "false";
        tester->executeWaitVerifyContent(status, "502 Query aborted");
    }

    void deleteNoWhereTrue() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "DELETE FROM foo;";
        status["nowhere"] = "true";
        tester->executeWaitVerifyContent(status);
    }

    void deleteWithWhere() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "INSERT INTO foo VALUES ( 666 );";
        tester->executeWaitVerifyContent(status);

        status["query"] = "DELETE FROM foo WHERE bar = 666;";
        tester->executeWaitVerifyContent(status);
    }

    void update() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "INSERT INTO foo VALUES ( 666 );";
        tester->executeWaitVerifyContent(status);

        status["query"] = "UPDATE foo SET bar = 777 WHERE bar = 666;";
        tester->executeWaitVerifyContent(status);
    }

    void failedUpdateNoWhere() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "UPDATE foo SET bar = 0;";
        tester->executeWaitVerifyContent(status, "502 Query aborted");
    }

    void failedUpdateNoWhereFalse() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "UPDATE foo SET bar = 0;";
        status["nowhere"] = "false";
        tester->executeWaitVerifyContent(status, "502 Query aborted");
    }

    void failedUpdateNoWhereTrue() {
        SData status("Query");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "UPDATE foo SET bar = 0;";
        status["nowhere"] = "true";
        tester->executeWaitVerifyContent(status);
    }

    void updateAndInsertWithHttp() {
        SData status("Query / HTTP/1.1");
        status["writeConsistency"] = "ASYNC";
        status["query"] = "INSERT INTO foo VALUES ( 666 );";
        tester->executeWaitVerifyContent(status);

        status["query"] = "UPDATE foo SET bar = 777 WHERE bar = 666;";
        tester->executeWaitVerifyContent(status);
    }

} __WriteTest;
