#include <test/lib/BedrockTester.h>

struct ReadTest : tpunit::TestFixture {
    ReadTest()
        : tpunit::TestFixture("Read",
                              BEFORE_CLASS(ReadTest::setup),
                              TEST(ReadTest::simpleRead),
                              TEST(ReadTest::readNoSemicolon),
                              AFTER_CLASS(ReadTest::tearDown)) { }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    void simpleRead() {
        SData status("Query");
        status["query"] = "SELECT 1;";
        string response = tester->executeWait(status);
        int val = SToInt(response);
        ASSERT_EQUAL(val, 1);
    }

    void readNoSemicolon() {
        SData status("Query");
        status["query"] = "SELECT 1";
        tester->executeWait(status, "502 Query failed");
    }

} __ReadTest;
