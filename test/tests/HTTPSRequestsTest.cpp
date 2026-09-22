#include <BedrockCommand.h>
#include <libstuff/JSON/Value.h>
#include <test/lib/tpunit++.hpp>

struct HTTPSRequestsTest : tpunit::TestFixture
{
    HTTPSRequestsTest()
        : tpunit::TestFixture("HTTPSRequests",
                              TEST(HTTPSRequestsTest::roundTrip),
                              TEST(HTTPSRequestsTest::invalidInput))
    {
    }

    void roundTrip()
    {
        SData fullRequest("GET /example HTTP/1.1");
        fullRequest["Host"] = "example.com";
        SData fullResponse("HTTP/1.1 200 OK");
        fullResponse.content = "Response body";
        JSON::Value transaction({
            {"created", 123},
            {"finished", "456"},
            {"timeoutAt", 789},
            {"response", "200"},
            {"fullRequest", SEncodeBase64(fullRequest.serialize())},
            {"fullResponse", SEncodeBase64(fullResponse.serialize())},
        });
        BedrockCommand command(SQLiteCommand{}, nullptr);
        command.deserializeHTTPSRequests(JSON::Value::singleItemArray(move(transaction)).serialize());
        ASSERT_EQUAL(command.httpsRequests.size(), 1);
        const auto& restored = *command.httpsRequests.begin();
        ASSERT_EQUAL(restored->created, 123);
        ASSERT_EQUAL(restored->finished, 456);
        ASSERT_EQUAL(restored->timeoutAt, 789);
        ASSERT_EQUAL(restored->response, 200);
        ASSERT_EQUAL(restored->fullRequest.serialize(), fullRequest.serialize());
        ASSERT_EQUAL(restored->fullResponse.serialize(), fullResponse.serialize());

        BedrockCommand roundTripped(SQLiteCommand{}, nullptr);
        roundTripped.deserializeHTTPSRequests(command.serializeHTTPSRequests());
        ASSERT_EQUAL(roundTripped.serializeHTTPSRequests(), command.serializeHTTPSRequests());
    }

    void invalidInput()
    {
        for (const string& invalid : {"", "[", "{}", "null", "[{\"response\":200}] trailing"}) {
            BedrockCommand command(SQLiteCommand{}, nullptr);
            command.deserializeHTTPSRequests(invalid);
            ASSERT_EQUAL(command.httpsRequests.size(), 0);
        }

        BedrockCommand invalidSuffix(SQLiteCommand{}, nullptr);
        invalidSuffix.deserializeHTTPSRequests(string("[{\"response\":200}]") + '\0' + "trailing");
        ASSERT_EQUAL(invalidSuffix.httpsRequests.size(), 0);

        // Missing fields and non-object entries retain the old incomplete-transaction defaults.
        BedrockCommand command(SQLiteCommand{}, nullptr);
        command.deserializeHTTPSRequests("[{},null,123]");
        ASSERT_EQUAL(command.httpsRequests.size(), 3);
        for (const auto& transaction : command.httpsRequests) {
            ASSERT_EQUAL(transaction->created, 0);
            ASSERT_EQUAL(transaction->finished, 0);
            ASSERT_EQUAL(transaction->timeoutAt, 0);
            ASSERT_EQUAL(transaction->response, 0);
            ASSERT_TRUE(transaction->fullRequest.methodLine.empty());
            ASSERT_TRUE(transaction->fullResponse.methodLine.empty());
        }
    }
} __HTTPSRequestsTest;
