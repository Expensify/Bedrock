#include "../BedrockClusterTester.h"

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
 *     {brcluster_node_0/MASTERING} ROLLBACK, conflicted on sync: brcluster_node_0#109 : sendrequest
 */
struct f_HTTPSTest : tpunit::TestFixture {
    f_HTTPSTest()
        : tpunit::TestFixture("f_HTTPSTest",
                              TEST(f_HTTPSTest::test)) { }

    BedrockClusterTester* tester;
    void test()
    {
        // Get the global tester object.
        tester = BedrockClusterTester::testers.front();

        // Send one request to verify that it works.
        BedrockTester* brtester = tester->getBedrockTester(0);

        SData request("sendrequest");
        auto result = brtester->executeWait(request);
        ASSERT_TRUE(result.size() > 10);

        // Now we spam a bunch of commands at the cluster, with one of them being an HTTPS reqeust command, and attempt
        // to cause a conflict.
        atomic<int> cmdID;
        cmdID.store(700);
        mutex m;

        // Let's spin up three threads, each spamming commands at one of our nodes.
        list<thread> threads;
        vector<vector<pair<string,string>>> responses(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &cmdID, &responses, &m](){
                BedrockTester* brtester = tester->getBedrockTester(i);
                vector<SData> requests;
                for (int j = 0; j < 200; j++) {
                    if (i == 0 && (j % 20 == 0)) {
                        // Every 10th request on master is an HTTP request.
                        // They should throw all sorts of errors if they repeat HTTPS requests.
                        SData request("sendrequest");
                        int cmdNum = cmdID.fetch_add(1);
                        request["Query"] = "INSERT INTO test VALUES(" + SQ(cmdNum) + ", " + SQ("HTTPS_TEST") + ");";
                        requests.push_back(request);
                    } else {
                        SData request("Query");
                        request["writeConsistency"] = "ASYNC";
                        int cmdNum = cmdID.fetch_add(1);
                        request["query"] = "INSERT INTO test VALUES ( " + SQ(cmdNum) + ", " + SQ("dummy") + ");";
                        requests.push_back(request);
                    }
                }
                auto results = brtester->executeWaitMultiple(requests);
                lock_guard<decltype(m)> lock(m);
                responses[i] = results;
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        // Ok, hopefully that caused a conflict, but everything succeeded. Let's see.
        // First, do we have all the rows we wanted?
        SData cmd("Query");
        cmd["Query"] = "SELECT COUNT(*) FROM test WHERE id >= 699;";

        // We should have received 600 new rows.
        string response = brtester->executeWait(cmd); 

        // Discard the header and parse the first line.
        list<string> lines;
        SParseList(response, lines, '\n');
        lines.pop_front();
        ASSERT_EQUAL(SToInt(lines.front()), 600);

        // And look at all the responses from master, to make sure they're all 200s, and either had a body or did not,
        // according with what sort of command they were.
        for (size_t i = 0; i < responses[0].size(); i++) {
            string code = responses[0][i].first;
            string body = responses[0][i].second;
            if (i % 20 == 0) {
                ASSERT_TRUE(body.size() > 10);
            } else {
                ASSERT_EQUAL(body.size(), 0);
            }
            ASSERT_EQUAL(SToInt(code), 200);
        }
    }
} __f_HTTPSTest;
