#include "../BedrockClusterTester.h"

struct BroadcastCommandTest : tpunit::TestFixture {
    BroadcastCommandTest()
        : tpunit::TestFixture("BroadcastCommand",
                              BEFORE_CLASS(BroadcastCommandTest::setup),
                              AFTER_CLASS(BroadcastCommandTest::teardown),
                              TEST(BroadcastCommandTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester(_threadID, "");
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        BedrockTester* master = tester->getBedrockTester(0);
        BedrockTester* slave = tester->getBedrockTester(1);

        // We want to test when this command runs.
        uint64_t now = STimeNow();

        // Make sure unhandled exceptions send the right response.
        SData cmd("broadcastwithtimeouts");
        try {
            master->executeWaitVerifyContent(cmd);
        } catch (...) {
            cout << "Couldn't send broadcastwithtimeouts" << endl;
            throw;
        }

        // Now wait for the slave to have received and run the command.
        sleep(5);

        SData cmd2("getbroadcasttimeouts");
        vector<SData> results;
        try {
            results = slave->executeWaitMultipleData({cmd2});
        } catch (...) {
            cout << "Couldn't send getbroadcasttimeouts" << endl;
            throw;
        }

        // The commandExecuteTime of the command should be between the time we stored as `now` above, and 3 seconds
        // after that
        ASSERT_GREATER_THAN(now + 3'000'000, stoull(results[0]["stored_commandExecuteTime"]));
        ASSERT_LESS_THAN(now, stoull(results[0]["stored_commandExecuteTime"]));

        // Other values should just match what's in the testplugin code.
        ASSERT_EQUAL(5001, stoll(results[0]["stored_processTimeout"]));
        ASSERT_EQUAL(5002, stoll(results[0]["stored_timeout"]));
        ASSERT_EQUAL("whatever", results[0]["stored_not_special"]);
    }

} __BroadcastCommandTest;
