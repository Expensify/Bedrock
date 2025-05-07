#include "libstuff/libstuff.h"
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

/* This test is inherently a non-conclusive test. It aims to submit conflicting HTTPS requests, but there's no simple
 * way to prove, conclusively that any of our commands actually conflicted. This test is constructed such that it has
 * a very high likelihood of causing a conflict (and in practice, seems to cause 8-10 on each run), but it's possible
 * that there were no conflicts, in which case the test succeeds but does not perform its main function, which is:
 *
 * What we really want to do here is validate that when a command with a HTTPS request conflicts, we retry processing
 * it without re-sending the HTTPS request. This is handled in testplugin's `sendrequest` command, where we'll
 * specifically aim to detect multiple instances of calling `peek` on this request, and throw an exception if we get to
 * process having done so more than once. There are also places in SQLiteNode that should assert on this condition.
 *
 * However, to actually verify that you saw a conflict during the test, you can look at the logs for something like:
 *
 * Feb 22 00:32:16 vagrant-ubuntu-trusty-64 bedrock: brcluster_node_0 (SQLiteNode.cpp:1298) update [sync] [warn]
 *     {brcluster_node_0/LEADING} ROLLBACK, conflicted on sync: brcluster_node_0#109 : sendrequest
 */
struct HTTPSTest : tpunit::TestFixture {
    HTTPSTest()
        : tpunit::TestFixture("HTTPS",
                              BEFORE_CLASS(HTTPSTest::setup),
                              AFTER_CLASS(HTTPSTest::teardown),
                              TEST(HTTPSTest::testMultipleRequests),
                              TEST(HTTPSTest::testWaitForTransactions),
                              TEST(HTTPSTest::test)) { }

    BedrockClusterTester* tester;

    void setup () {
        tester = new BedrockClusterTester();
    }

    void teardown() {
        delete tester;
    }

    void testMultipleRequests() {
        BedrockTester& brtester = tester->getTester(1);
        SData request("sendrequest");
        request["httpsRequestCount"] = to_string(3);
        auto result = brtester.executeWaitVerifyContent(request);
        auto lines = SParseList(result, '\n');
        ASSERT_EQUAL(lines.size(), 3);
    }

    void testWaitForTransactions() {
        BedrockTester& brtester = tester->getTester(1);
        SData request("httpswait");
        auto result = brtester.executeWaitMultipleData({request});
        ASSERT_TRUE(SStartsWith(result[0].methodLine, "200"));
    }

    void test() {
        // Send one request to verify that it works.
        BedrockTester& brtester = tester->getTester(1);

        SData request("sendrequest a");
        auto result = brtester.executeWaitVerifyContent(request);
        auto lines = SParseList(result, '\n');
        ASSERT_EQUAL(lines.size(), 1);

        // Now we spam a bunch of commands at the cluster, with one of them being an HTTPS reqeust command, and attempt
        // to cause a conflict.
        mutex m;

        // Every 10th request is an HTTP request.
        int nthHasRequest = 10;

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        vector<vector<SData>> responses(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, nthHasRequest, &responses, &m](){
                vector<SData> requests;
                for (int j = 0; j < 200; j++) {
                    if (j % nthHasRequest == 0) {
                        // They should throw all sorts of errors if they repeat HTTPS requests.
                        SData request("sendrequest b");
                        request["writeConsistency"] = "ASYNC";
                        requests.push_back(request);
                    } else {
                        SData request("idcollision f");
                        request["writeConsistency"] = "ASYNC";
                        requests.push_back(request);
                    }
                }
                auto results = tester->getTester(i).executeWaitMultipleData(requests);
                lock_guard<decltype(m)> lock(m);
                responses[i] = results;
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        // Look at all the responses from leader, to make sure they're all 200s, and either had a body or did not,
        // according with what sort of command they were.
        for (size_t i = 0; i < responses[0].size(); i++) {
            string code = responses[0][i].methodLine;
            string body = responses[0][i].content;
            if (i % nthHasRequest == 0) {
                ASSERT_TRUE(body.size());
            } else {
                ASSERT_FALSE(body.size());
            }
            if (SToInt(code) != 200) {
                cout << "[HTTPSTest] Bad code: " << code << endl;
            }
            ASSERT_EQUAL(SToInt(code), 200);
        }
    }
} __HTTPSTest;
