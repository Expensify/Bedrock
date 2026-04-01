#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

struct SignalExceptionTest : tpunit::TestFixture
{
    SignalExceptionTest()
        : tpunit::TestFixture("SignalException",
                              TEST(SignalExceptionTest::testThreadSIGSEGV),
                              TEST(SignalExceptionTest::testThreadSIGFPE),
                              TEST(SignalExceptionTest::testSocketThreadSIGSEGV))
    {
    }

    // Test SIGSEGV in an SThread is caught and propagated
    void testThreadSIGSEGV()
    {
        BedrockClusterTester tester;

        // Without rethrow - thread catches exception internally
        SData command("ThreadSIGSEGV");
        SData result = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_EQUAL(result.methodLine, "200 OK");

        // With rethrow - verify exception is caught as SSignalException
        command["rethrow"] = "true";
        result = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_EQUAL(result.methodLine, "200 Caught SIGSEGV");
    }

    // Test SIGFPE in an SThread is caught and propagated
    void testThreadSIGFPE()
    {
        BedrockClusterTester tester;

        SData command("ThreadSIGFPE");
        command["rethrow"] = "true";
        SData result = tester.getTester(0).executeWaitMultipleData({command})[0];
        ASSERT_EQUAL(result.methodLine, "200 Caught SIGFPE");
    }

    // Test SIGSEGV in socket thread (via generatesegfaultpeek) now recovers
    // instead of crashing. Server should remain running.
    void testSocketThreadSIGSEGV()
    {
        BedrockClusterTester tester;

        // This command triggers SIGSEGV in the socket handler thread.
        // With signal recovery, the thread should exit gracefully and
        // the server should continue running.
        SData command("generatesegfaultpeek");
        SData result = tester.getTester(0).executeWaitMultipleData({command})[0];

        // The socket will close due to the exception, but server stays up.
        // We might get a connection error or empty response.
        // The key test is that the server is still alive:

        SData pingCommand("Status");
        SData pingResult = tester.getTester(0).executeWaitMultipleData({pingCommand})[0];
        ASSERT_EQUAL(pingResult.methodLine, "200 OK");
    }
} __SignalExceptionTest;
