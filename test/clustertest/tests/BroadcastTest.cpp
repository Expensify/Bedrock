#include <iostream>

#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct BroadcastCommandTest : tpunit::TestFixture {
    BroadcastCommandTest()
        : tpunit::TestFixture("BroadcastCommand",
                              BEFORE_CLASS(BroadcastCommandTest::setup),
                              AFTER_CLASS(BroadcastCommandTest::teardown),
                              TEST(BroadcastCommandTest::test)
                             ) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester();
    }

    void teardown() {
        delete tester;
    }

    void test()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);

        // We want to test when this command runs.
        uint64_t now = STimeNow();

        // Make sure unhandled exceptions send the right response.
        SData cmd("broadcastwithtimeouts");
        try {
            leader.executeWaitVerifyContent(cmd);
        } catch (...) {
            cout << "[BroadcastTest] Couldn't send broadcastwithtimeouts" << endl;
            throw;
        }

        // Now wait for the follower to have received and run the command.
        sleep(5);

        SData cmd2("getbroadcasttimeouts");
        vector<SData> results;
        try {
            results = follower.executeWaitMultipleData({cmd2});
        } catch (...) {
            cout << "[BroadcastTest] Couldn't send getbroadcasttimeouts" << endl;
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
