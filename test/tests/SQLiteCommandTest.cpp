#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteCommand.h>
#include <test/lib/tpunit++.hpp>

struct SQLiteCommandTest : tpunit::TestFixture
{
    SQLiteCommandTest() : tpunit::TestFixture("SQLiteCommand",
                                              TEST(SQLiteCommandTest::testPreprocessingMoveConstructor))
    {
    }

    static void lowercaseName(SData& request)
    {
        request["name"] = SToLower(request["name"]);
    }

    void testPreprocessingMoveConstructor()
    {
        SData request("TestCommand");
        request["name"] = "MixedCase";
        SQLiteCommand command(move(request));
        command.initiatingClientID = 5;
        command.id = "commandID";
        command.escalated = true;
        const string requestID = command.request["requestID"];
        const uint64_t creationTime = command.creationTime;

        SQLiteCommand preprocessedCommand(move(command), lowercaseName);

        // The preprocessor can change the request.
        ASSERT_EQUAL(preprocessedCommand.request["name"], "mixedcase");

        // Everything else carries over from the command it was moved from.
        ASSERT_EQUAL(preprocessedCommand.request.methodLine, "TestCommand");
        ASSERT_EQUAL(preprocessedCommand.request["requestID"], requestID);
        ASSERT_EQUAL(preprocessedCommand.initiatingClientID, 5);
        ASSERT_EQUAL(preprocessedCommand.id, "commandID");
        ASSERT_TRUE(preprocessedCommand.escalated);
        ASSERT_EQUAL(preprocessedCommand.creationTime, creationTime);
    }
} __SQLiteCommandTest;
