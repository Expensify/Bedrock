#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct StatusTest : tpunit::TestFixture {
    StatusTest() : tpunit::TestFixture("Status") {
        registerTests(TEST(StatusTest::test));
    }

    void test() {
        BedrockTester tester;
        SData status("Status");
        string response = tester.executeWaitMultipleData({status})[0].content;
        ASSERT_TRUE(SContains(response, "plugins"));
        ASSERT_TRUE(SContains(response, "multiWriteManualBlacklist"));
    }

} __StatusTest;
