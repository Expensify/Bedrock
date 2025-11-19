#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct CommandPortTest : tpunit::TestFixture
{
    CommandPortTest()
        : tpunit::TestFixture("CommandPort", TEST(CommandPortTest::test))
    {
    }

    void test()
    {
        BedrockTester tester;

        // When we close the command port with a reason
        SData closeCommandPort("SuppressCommandPort");
        closeCommandPort["reason"] = "testCommandPort";
        STable response = SParseJSONObject(tester.executeWaitMultipleData({closeCommandPort})[0].content);

        // The status command should show it in commandPortBlockReasons
        SData status("Status");
        response = SParseJSONObject(tester.executeWaitMultipleData({status}, 10, true)[0].content);
        ASSERT_EQUAL(SParseJSONArray(response["commandPortBlockReasons"]), list<string>{"testCommandPort"});

        // When we run ClearCommandPort with a reason different from the one used to close it
        SData badClearCommandPort("ClearCommandPort");
        tester.executeWaitMultipleData({badClearCommandPort}, 10, true);

        // The command port should stay close and the status command should still show the reason the port is closed in commandPortBlockReasons
        response = SParseJSONObject(tester.executeWaitMultipleData({status}, 10, true)[0].content);
        ASSERT_EQUAL(SParseJSONArray(response["commandPortBlockReasons"]), list<string>{"testCommandPort"});

        // When we run ClearCommandPort with the same reason as the one used to close it
        SData clearCommandPort("ClearCommandPort");
        clearCommandPort["reason"] = "testCommandPort";
        tester.executeWaitMultipleData({clearCommandPort}, 10, true);

        // Then the command port should open and the reason should be removed from commandPortBlockReasons
        response = SParseJSONObject(tester.executeWaitMultipleData({status})[0].content);
        ASSERT_EQUAL(SParseJSONArray(response["commandPortBlockReasons"]), list<string>{});
    }
} __CommandPortTest;
