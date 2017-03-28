#include <test/lib/BedrockTester.h>

struct WriteTest : tpunit::TestFixture {
    WriteTest()
        : tpunit::TestFixture("Write",
                              BEFORE_CLASS(WriteTest::setup),
                              TEST(WriteTest::insert),
                              TEST(WriteTest::parallelInsert),
                              AFTER_CLASS(WriteTest::tearDown)) { }

    BedrockTester* tester;


    list<string> queries = {
        "CREATE TABLE foo (bar INTEGER);",
        "CREATE TABLE stuff (id INTEGER PRIMARY KEY, value INTEGER);",
    };

    void setup() {
        tester = new BedrockTester("", "", queries);
    }

    void tearDown() {
        delete tester;
    }

    void insert() {
        for (int i = 0; i < 50; i++) {
            SData status("Query");
            status["writeConsistency"] = "ASYNC";
            status["query"] = "INSERT INTO foo VALUES ( RANDOM() );";
            tester->executeWait(status);
        }

        SData status("Query");
        status["query"] = "SELECT COUNT(*) FROM foo;";
        string response = tester->executeWait(status);
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
        auto results = tester->executeWaitMultiple(requests);

        int success = 0;
        int failure = 0;

        for (auto& row : results) {
            if (SToInt(row.first) == 200) {
                success++;
            } else {
                failure++;
            }
        }

        ASSERT_EQUAL(success, numCommands);
    }

} __WriteTest;
