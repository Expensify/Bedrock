/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    PrePeekPostProcessTest.cpp
 * Path:    test/clustertest/tests/PrePeekPostProcessTest.cpp
 *
 * INTENT
 *   Cluster test verifying testplugin commands can hook prePeek/postProcess
 *   in addition to peek/process, that data returned from each stage reaches
 *   the client response, and that DB writes made in process() are visible
 *   in postProcess() but not in peek() (before) or prePeek() (before that).
 *
 * OBJECTS
 *   checkWithoutThis()               - file-local free function; a no-op
 *                        tautological assertion (ASSERT_EQUAL(1, 1)) called
 *                        once from prePeek() to confirm a free function can
 *                        be called from a fixture method with no `this`
 *   PrePeekPostProcessTest            - tpunit fixture
 *   PrePeekPostProcessTest::setup/teardown - own the BedrockClusterTester
 *   PrePeekPostProcessTest::prePeek        - prepeekcommand: checks prePeek/
 *                        peek info round-trip and the row isn't inserted yet
 *   PrePeekPostProcessTest::prePeekThrow   - prepeekcommand w/ shouldThrow:
 *                        checks the resulting "501 ERROR"
 *   PrePeekPostProcessTest::postProcess    - postprocesscommand: checks
 *                        peek/process/postProcess info round-trip and that
 *                        a row inserted during process() is absent at peek
 *                        time but present at postProcess time
 *   PrePeekPostProcessTest::prePeekPostProcess - prepeekpostprocesscommand:
 *                        same, but the row is deleted during process(), so
 *                        it's present at peek time and gone at postProcess
 *
 * OUT OF PLACE
 *   [CANDIDATE] checkWithoutThis(): a free function asserting a hardcoded
 *   tautology, not exercising any Bedrock behavior itself. It reads as a
 *   leftover probe (e.g. confirming a plain function is callable from a
 *   fixture method) rather than part of the test's stated intent.
 *
 * NAME/LOCATION FIT
 *   Fits: a prePeek/postProcess hook test alongside its peers.
 *
 * NAMING QUALITY
 *   Clear method names; checkWithoutThis's name doesn't convey what it does
 *   or why it exists.
 * ─────────────────────────────────────────────────────────────────────*/
#include <BedrockCommand.h>
#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

void checkWithoutThis()
{
    ASSERT_EQUAL(1, 1);
}

struct PrePeekPostProcessTest : tpunit::TestFixture
{
    PrePeekPostProcessTest() : tpunit::TestFixture("PrePeekPostProcess", BEFORE_CLASS(PrePeekPostProcessTest::setup),
                                                   AFTER_CLASS(PrePeekPostProcessTest::teardown),
                                                   TEST(PrePeekPostProcessTest::prePeek),
                                                   TEST(PrePeekPostProcessTest::prePeekThrow),
                                                   TEST(PrePeekPostProcessTest::postProcess),
                                                   TEST(PrePeekPostProcessTest::prePeekPostProcess))
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

    void prePeek()
    {
        checkWithoutThis();

        BedrockTester& brtester = tester->getTester(1);
        SData cmd("prepeekcommand");
        STable response = SParseJSONObject(brtester.executeWaitMultipleData({cmd})[0].content);

        // Confirm that information returned from prePeek and peek is all in the response.
        ASSERT_EQUAL(response["prePeekInfo"], "this was returned in prePeekInfo");
        ASSERT_EQUAL(response["peekInfo"], "this was returned in peekInfo");

        // No counted row has been inserted into the test table yet, so the "peekCount" should be zero.
        ASSERT_EQUAL(response["peekCount"], "0");
    }

    void prePeekThrow()
    {
        BedrockTester& brtester = tester->getTester(1);
        SData cmd("prepeekcommand");
        cmd["shouldThrow"] = "true";
        brtester.executeWaitVerifyContent({cmd}, "501 ERROR");
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

    void prePeekPostProcess()
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
