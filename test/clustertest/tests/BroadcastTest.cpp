#include "test/lib/tpunit++.hpp"
#include <iostream>

#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct BroadcastCommandTest : tpunit::TestFixture
{
    BroadcastCommandTest()
        : tpunit::TestFixture("BroadcastCommand",
                              BEFORE_CLASS(BroadcastCommandTest::setup),
                              AFTER_CLASS(BroadcastCommandTest::teardown),
                              TEST(BroadcastCommandTest::test)
        )
    {
    }

    BedrockClusterTester* tester;

    void setup()
    {
        tester = new BedrockClusterTester();
    }

    void teardown()
    {
        delete tester;
    }

    void test()
    {
        BedrockTester& leader = tester->getTester(0);
        BedrockTester& follower = tester->getTester(1);
        BedrockTester& follower2 = tester->getTester(2);

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
        vector<SData> results2;
        try {
            results = follower.executeWaitMultipleData({cmd2});
            results2 = follower2.executeWaitMultipleData({cmd2});
        } catch (...) {
            cout << "[BroadcastTest] Couldn't send getbroadcasttimeouts" << endl;
            throw;
        }

        // The peekedAt of the command should be between the time we stored as `now` above, and 3 seconds
        // after that
        ASSERT_GREATER_THAN(now + 3'000'000, stoull(results[0]["stored_peekedAt"]));
        ASSERT_LESS_THAN(now, stoull(results[0]["stored_peekedAt"]));

        // Other values should just match what's in the testplugin code.
        ASSERT_EQUAL(5001, stoll(results[0]["stored_processTimeout"]));

        // We set this to 6000, it could be slightly less than that because timeouts are adjusted to allow for time elapsed when sending.
        // so, 5999 or something like that is reasonable to see.
        ASSERT_LESS_THAN(5000, stoll(results2[0]["stored_timeout"]));
        ASSERT_GREATER_THAN_EQUAL(6000, stoll(results2[0]["stored_timeout"]));
        ASSERT_EQUAL("whatever", results[0]["stored_not_special"]);

        // Verify the results were received on cluster node 3 (follower 2) as well
        ASSERT_GREATER_THAN(now + 3'000'000, stoull(results2[0]["stored_peekedAt"]));
        ASSERT_LESS_THAN(now, stoull(results2[0]["stored_peekedAt"]));
        ASSERT_EQUAL(5001, stoll(results2[0]["stored_processTimeout"]));

        // We set this to 6000, it could be slightly less than that because timeouts are adjusted to allow for time elapsed when sending.
        // so, 5999 or something like that is reasonable to see.
        ASSERT_LESS_THAN(5000, stoll(results2[0]["stored_timeout"]));
        ASSERT_GREATER_THAN_EQUAL(6000, stoll(results2[0]["stored_timeout"]));
        ASSERT_EQUAL("whatever", results2[0]["stored_not_special"]);
    }
} __BroadcastCommandTest;
