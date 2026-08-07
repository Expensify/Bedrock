#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>

struct GetCommitHashTest : tpunit::TestFixture
{
    GetCommitHashTest()
        : tpunit::TestFixture("GetCommitHash",
                              TEST(GetCommitHashTest::testValidation),
                              TEST(GetCommitHashTest::testReturnsJournalHash),
                              TEST(GetCommitHashTest::testAllowedFromRemoteHost))
    {
    }

    // Makes a commit and returns the commit count it landed at.
    string commitSomething(BedrockTester& tester, const string& tableName)
    {
        SData query("Query");
        query["query"] = "CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY);";
        tester.executeWaitVerifyContent(query);

        STable status = SParseJSONObject(tester.executeWaitVerifyContent(SData("Status"), "200", true));
        return status["CommitCount"];
    }

    // Returns the hash the journal actually holds for `commitCount`, by looking through every journal table for it.
    string journalHash(BedrockTester& tester, const string& commitCount)
    {
        SQResult journals;
        tester.readDB("SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE 'journal%';", journals);
        for (auto& row : journals) {
            SQResult hash;
            tester.readDB("SELECT hash FROM " + row[0] + " WHERE id = " + commitCount + ";", hash);
            if (!hash.empty()) {
                return hash[0][0];
            }
        }
        return "";
    }

    void testValidation()
    {
        BedrockTester tester;

        tester.executeWaitVerifyContent(SData("GetCommitHash"), "400 Missing commitCount", true);

        SData zero("GetCommitHash");
        zero["commitCount"] = "0";
        tester.executeWaitVerifyContent(zero, "400 Invalid commitCount", true);

        SData garbage("GetCommitHash");
        garbage["commitCount"] = "not a number";
        tester.executeWaitVerifyContent(garbage, "400 Invalid commitCount", true);

        SData tooBig("GetCommitHash");
        tooBig["commitCount"] = "1000000";
        tester.executeWaitVerifyContent(tooBig, "404 Commit not found", true);
    }

    void testReturnsJournalHash()
    {
        BedrockTester tester;
        const string commitCount = commitSomething(tester, "getCommitHashTest");
        ASSERT_FALSE(commitCount.empty());

        SData get("GetCommitHash");
        get["commitCount"] = commitCount;
        vector<SData> responses = tester.executeWaitMultipleData({get}, 1, true);
        ASSERT_EQUAL(responses[0].methodLine, "200 OK");
        ASSERT_EQUAL(responses[0]["commitCount"], commitCount);

        const string expected = journalHash(tester, commitCount);
        ASSERT_FALSE(expected.empty());
        ASSERT_EQUAL(responses[0]["hash"], expected);
    }

    void testAllowedFromRemoteHost()
    {
        BedrockTester tester;
        const string commitCount = commitSomething(tester, "getCommitHashRemoteTest");

        // A populated `_source` is what the server uses to decide a command didn't originate on localhost. Most
        // control commands are rejected in that case, but GetCommitHash is allowed.
        SData get("GetCommitHash");
        get["commitCount"] = commitCount;
        get["_source"] = "10.0.0.1";
        vector<SData> responses = tester.executeWaitMultipleData({get}, 1, true);
        ASSERT_EQUAL(responses[0].methodLine, "200 OK");
        ASSERT_FALSE(responses[0]["hash"].empty());

        SData report("ConflictReport");
        report["_source"] = "10.0.0.1";
        tester.executeWaitVerifyContent(report, "401 Unauthorized", true);
    }
} __GetCommitHashTest;
