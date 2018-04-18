#include "../BedrockClusterTester.h"
#include <fstream>

struct abandonedCommandTest : tpunit::TestFixture {
    abandonedCommandTest()
        : tpunit::TestFixture("abandonedCommandTest",
                              BEFORE_CLASS(abandonedCommandTest::setup),
                              AFTER_CLASS(abandonedCommandTest::teardown),
                              TEST(abandonedCommandTest::abandon)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID);
    }

    void teardown() {
        delete tester;
    }


    void abandon() {

        // Send three commands (one to each node) and immediately disconnect after each.
        list<thread> threads;
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i](){

                BedrockTester* brtester = tester->getBedrockTester(i);
                int socket = S_socket(brtester->getServerAddr(), true, false, true);

                SData request("Query");
                request["writeConsistency"] = "ASYNC";
                request["Query"] = "INSERT INTO test VALUES(" + SQ(i + 600) + ", " + SQ("abandon_" + to_string(i)) + ");";

                string sendBuffer = request.serialize();
                // Send our data.
                while (sendBuffer.size()) {
                    bool result = S_sendconsume(socket, sendBuffer);
                    if (!result) {
                        break;
                    }
                }

                // Abandon the request.
                close(socket);
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();


        // Wait one second.
        sleep(1);

        // Send another command to each node. They should all succeed. Any number of the original commands might have
        // completed.

        mutex m;
        vector<string> results(3);
        for (int i : {0, 1, 2}) {
            threads.emplace_back([this, i, &results, &m](){

                BedrockTester* brtester = tester->getBedrockTester(i);

                SData query("Query");
                query["Query"] = "SELECT value FROM test WHERE id >= 600;";

                string result = brtester->executeWaitVerifyContent(query);
                SAUTOLOCK(m);
                results[i] = result;
            });
        }

        // Done.
        for (thread& t : threads) {
            t.join();
        }
        threads.clear();

        ASSERT_EQUAL(results[0], results[1]);
        ASSERT_EQUAL(results[1], results[2]);
    }

} __abandonedCommandTest;
