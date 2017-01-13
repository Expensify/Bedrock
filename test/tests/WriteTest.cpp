#include <test/lib/BedrockTester.h>

struct WriteTest : tpunit::TestFixture {
    WriteTest()
        : tpunit::TestFixture(BEFORE_CLASS(WriteTest::setup), TEST(WriteTest::create), TEST(WriteTest::insert),
                              AFTER_CLASS(WriteTest::tearDown)) {
        NAME(Write);
    }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    void create() {
        SData status("Query");
        // We can't create a table with BEGIN CONCURRENT.
        status["query"] = "CREATE TABLE foo (bar INTEGER);";
        tester->executeWait(status);
    }

    void insert() {
        for (int i = 0; i < 50; i++) {
            SData status("Query");
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

} __WriteTest;
