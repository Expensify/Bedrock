#include <test/lib/BedrockTester.h>

struct ParallelConnectionTest : tpunit::TestFixture {
    ParallelConnectionTest()
        : tpunit::TestFixture("ParallelConnectionTest",
                              BEFORE_CLASS(ParallelConnectionTest::setup),
                              TEST(ParallelConnectionTest::parallelStatus),
                              TEST(ParallelConnectionTest::parallelInsert),
                              AFTER_CLASS(ParallelConnectionTest::tearDown)) { }

    BedrockTester* tester;

    void setup() {
        tester = new BedrockTester(_threadID, {}, {
            "CREATE TABLE foo (bar INTEGER);",
            "CREATE TABLE stuff (id INTEGER PRIMARY KEY, value INTEGER);",
        });
    }

    void tearDown() {
        delete tester;
    }

    void parallelStatus() {
        vector<SData> requests;
        int numCommands = 100;
        for (int i = 0; i < numCommands; i++) {
            SData query("Status");
            query["debugID"] = "parallelCommand#" + to_string(i);
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
    }

    void parallelInsert() {
        vector<SData> requests;
        int numCommands = 100;
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
        SData query("Query");
        query["query"] = "SELECT COUNT(*) FROM stuff;";
        string response = tester->executeWaitVerifyContent(query);
        // Skip the header line.
        string secondLine = response.substr(response.find('\n') + 1);
        int val = SToInt(secondLine);
        ASSERT_EQUAL(val, numCommands);
    }

} __ParallelConnectionTest;
