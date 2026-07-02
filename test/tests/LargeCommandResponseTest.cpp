#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

// Coverage/sanity test for the fix described in the "Bedrock response send-drain fix" plan: verifies a real
// BedrockServer, hit over the real command port, delivers a multi-MB command response to the client
// uncorrupted and untruncated.
//
// Unlike SocketSendDrainTest, this doesn't force the triggering condition (a signal interrupting the
// blocking send()), so on its own it isn't a reliable regression guard for the truncation bug -- on a
// healthy loopback connection a single blocking send() call typically blocks internally until everything is
// queued. What it does exercise is the full real pipeline (plugin -> BedrockCommand -> serialization ->
// _reply() -> command port) that SocketSendDrainTest doesn't touch at all.
struct LargeCommandResponseTest : tpunit::TestFixture
{
    LargeCommandResponseTest()
        : tpunit::TestFixture("LargeCommandResponse", TEST(LargeCommandResponseTest::testMultiMegabyteResponse))
    {
    }

    void testMultiMegabyteResponse()
    {
        BedrockTester tester;

        // hex(zeroblob(N)) deterministically produces a string of 2*N '0' characters, so completeness and
        // correctness can both be verified without needing a separately-computed reference payload.
        const size_t blobBytes = 5 * 1024 * 1024;
        SData query("Query");
        query["ReadDBFlags"] = "-json";
        query["query"] = "SELECT hex(zeroblob(" + to_string(blobBytes) + ")) AS bigValue;";

        string resultJSON = tester.executeWaitMultipleData({query}, 1)[0].content;
        string bigValue = SParseJSONObject(SParseJSONArray(resultJSON).front())["bigValue"];

        ASSERT_EQUAL(bigValue.size(), blobBytes * 2);
        ASSERT_EQUAL(bigValue.find_first_not_of('0'), string::npos);
    }
} __LargeCommandResponseTest;
