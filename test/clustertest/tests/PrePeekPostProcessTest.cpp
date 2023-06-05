#include <BedrockCommand.h>
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct PrePeekPostProcessTest : tpunit::TestFixture {
    PrePeekPostProcessTest() : tpunit::TestFixture("PrePeekPostProcess", BEFORE_CLASS(PrePeekPostProcessTest::setup),
                                                                         AFTER_CLASS(PrePeekPostProcessTest::teardown),
                                                                         TEST(PrePeekPostProcessTest::prePeek),
                                                                         TEST(PrePeekPostProcessTest::postProcess),
                                                                         TEST(PrePeekPostProcessTest::prePeekPostProcess)) { }

    BedrockClusterTester* tester;

    void setup() {
        tester = new BedrockClusterTester();
    }

    void teardown() {
        delete tester;
    }

    void prePeek()
    {
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("prepeekcommand");
        STable response = SParseJSONObject(brtester.executeWaitMultipleData({cmd})[0].content);

        // Confirm that information returned from prePeek and peek is all in the response.
        ASSERT_EQUAL(response["prePeekInfo"], "this was returned in prePeekInfo");
        ASSERT_EQUAL(response["peekInfo"], "this was returned in peekInfo");

        // No counted row has been inserted into the test table yet, so the "peekCount" should be zero.
        ASSERT_EQUAL(response["peekCount"], "0");
    }

    void postProcess()
    {
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("postprocesscommand");
        STable response = SParseJSONObject(brtester.executeWaitMultipleData({cmd})[0].content);

        // Confirm that the information returned from peek, process and postProcess is all in the response.
        ASSERT_EQUAL(response["peekInfo"], "this was returned in peekInfo");
        ASSERT_EQUAL(response["processInfo"], "this was returned in processInfo");
        ASSERT_EQUAL(response["postProcessInfo"], "this was returned in postProcessInfo");

        // postprocesscommand inserts a row in the "test" table during the process. We need to make sure that the
        // inserted row does not exist during peek, and that it does exist during postProcess.
        ASSERT_EQUAL(response["peekCount"], "0");
        ASSERT_EQUAL(response["postProcessCount"], "1");
    }

    void prePeekPostProcess ()
    {
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("prepeekpostprocesscommand");
        STable response = SParseJSONObject(brtester.executeWaitMultipleData({cmd})[0].content);

        // Confirm that the information returned from prePeek, peek, process and postProcess is all in the response.
        ASSERT_EQUAL(response["prePeekInfo"], "this was returned in prePeekInfo");
        ASSERT_EQUAL(response["peekInfo"], "this was returned in peekInfo");
        ASSERT_EQUAL(response["processInfo"], "this was returned in processInfo");
        ASSERT_EQUAL(response["postProcessInfo"], "this was returned in postProcessInfo");

        // prepeekpostprocesscommand deletes a row from the "test" table during the process. We need to make sure the
        // row exists during peek, and that it no longer exists during postProcess.
        ASSERT_EQUAL(response["peekCount"], "1");
        ASSERT_EQUAL(response["postProcessCount"], "0");
    }

} __PrePeekPostProcessTest;

