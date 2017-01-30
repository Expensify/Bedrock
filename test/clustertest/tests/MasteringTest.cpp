#include "../BedrockClusterTester.h"

struct MasteringTest : tpunit::TestFixture {
    MasteringTest()
        : tpunit::TestFixture(
                              TEST(MasteringTest::test)
                             ) {
        NAME(Mastering);
    }

    void test() {

        // Get the global tester object.
        BedrockClusterTester* tester = BedrockClusterTester::testers.front();

        list<BedrockTester*> brtesters;
        for (auto i : {0, 1, 2}) {
            brtesters.push_back(tester->getBedrockTester(i));
        }

        list<string> results;

        // Get the status from each node.
        for (auto& brtest : brtesters) {
            SData cmd("Status");
            string response = brtest->executeWait(cmd);
            STable json = SParseJSONObject(response);
            results.push_back(json["state"]);
        }

        // Check that the first is mastering and the others are slaving.
        int current = 0;
        for (string result : results) {
            if (current) {
                ASSERT_EQUAL(result, "SLAVING");
            } else {
                ASSERT_EQUAL(result, "MASTERING");
            }
            current++;
        }
    }


} __MasteringTest;
