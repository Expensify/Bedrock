#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>

struct ChainedHTTPTest : tpunit::TestFixture
{
    ChainedHTTPTest()
        : tpunit::TestFixture("ChainedHTTP",
                              TEST(ChainedHTTPTest::test))
    {
    }

    void test()
    {
        // Load the clustertest testplugin that implements our chained command.
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }
        BedrockTester tester({
            {"-plugins", string(cwd) + "/clustertest/testplugin/testplugin.so"},
        });

        // Some of the biggest sites on the internet. These sites in particular were chosen by the fact that they
        // return 200s even with our super-simple request format.
        list<string> urls = {
            "www.google.com",
            "www.youtube.com",
            "www.amazon.com",
        };

        SData request("chainedrequest");
        request["urls"] = SComposeList(urls);
        vector<SData> result = tester.executeWaitMultipleData({request}, 1);

        // Verify we have a result.
        ASSERT_EQUAL(result.size(), 1);

        // Then parse and verify the response.
        list<string> results = SParseList(result[0].content, '\n');
        map<string, int64_t> resultMap;
        for (auto& site : urls) {
            resultMap.emplace(make_pair(site, -1));
        }
        for (auto& r : results) {
            list<string> split = SParseList(r, ':');
            if (split.size() == 2) {
                resultMap[split.front()] = stoll(split.back());
            } else if (split.size() == 1) {
                ASSERT_EQUAL(split.front(), "PROCESSED");
                break;
            } else {
                // We should have had exactly one or two results.
                ASSERT_TRUE(false);
            }
        }

        // At this point, we should have good result values in our map, verify that.
        for (auto& r : urls) {
            int64_t result = resultMap.at(r);

            // See if this looks like a non-error return code.
            ASSERT_TRUE(result > 0 && result < 400);
        }
    }
} __ChainedHTTPTest;
