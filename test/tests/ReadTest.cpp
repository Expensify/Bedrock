#include <test/lib/BedrockTester.h>

struct ReadTest : tpunit::TestFixture
{
    ReadTest() : tpunit::TestFixture(BEFORE_CLASS(ReadTest::setup),
                                           TEST(ReadTest::simpleRead),
                                           AFTER_CLASS(ReadTest::tearDown))
    {
        NAME(Read);
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
        string response = tester->executeWait(status);
        int val = SToInt(response);
        ASSERT_EQUAL(val, 1);
    }

} __ReadTest;
