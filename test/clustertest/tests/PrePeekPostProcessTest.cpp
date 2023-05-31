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
    }

    void postProcess()
    {
    }

    void prePeekPostProcess ()
    {
    }

} __PrePeekPostProcessTest;

