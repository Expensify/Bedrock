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
        ASSERT_EQUAL(response["prePeekInfo"], "this was returned in prePeekInfo");
    }

    void postProcess()
    {
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("postprocesscommand");
        STable response = SParseJSONObject(brtester.executeWaitMultipleData({cmd})[0].content);
        ASSERT_EQUAL(response["postProcessInfo"], "this was returned in postProcessInfo");
    }

    void prePeekPostProcess ()
    {
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("prepeekpostprocesscommand");
        STable response = SParseJSONObject(brtester.executeWaitMultipleData({cmd})[0].content);
        ASSERT_EQUAL(response["prePeekInfo"], "this was returned in prePeekInfo");
        ASSERT_EQUAL(response["postProcessInfo"], "this was returned in postProcessInfo");
    }

} __PrePeekPostProcessTest;

