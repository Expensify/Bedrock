#include "../BedrockClusterTester.h"
#include <fstream>

struct e_futureExecutionTest : tpunit::TestFixture {
    e_futureExecutionTest()
        : tpunit::TestFixture("e_futureExecution",
                              TEST(e_futureExecutionTest::futureExecution)) { }

    void futureExecution() {
        // We only care about master because future execution only works on Master.
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();
        BedrockTester* brtester = tester->getBedrockTester(0);

        // Let's run a command in the future.
        SData query("Query");
        // Three seconds from now.
        query["commandExecuteTime"] = to_string(STimeNow() + 3000000);
        query["Query"] = "INSERT INTO test VALUES(" + SQ(500) + ", " + SQ("sent_by_master") + ");";
        string result = brtester->executeWait(query, "202"); 

        // Ok, Now let's wait a second
        sleep(1);

        // And make it still hasn't been inserted.
        query.clear();
        query.methodLine = "Query";
        query["Query"] = "SELECT * FROM test WHERE id >= 500;";
        result = brtester->executeWait(query);

        ASSERT_FALSE(SContains(result, "500"));

        // Then sleep three more seconds, it *should* be there now.
        sleep(3);

        // And now it should be there.
        result = brtester->executeWait(query);

        ASSERT_TRUE(SContains(result, "500"));
    }

} __e_futureExecutionTest;
