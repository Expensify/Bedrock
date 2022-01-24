#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>

struct SupporessCommandPortCrashTest : tpunit::TestFixture {
    SupporessCommandPortCrashTest()
        : tpunit::TestFixture("SupporessCommandPortCrash", TEST(SupporessCommandPortCrashTest::test))
    { }

    void test() {
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }
        BedrockTester tester({{"-plugins", string(cwd) + "/clustertest/testplugin/testplugin.so"}});

        // We want two threads, one opening and closing the command port, and one spamming commands to read.

        // open and close the command port.
        thread t1([&tester](){
            for (int i = 0; i < 10'000; i++) {
                SData request("SuppressCommandPort");
                tester.executeWaitMultipleData({request}, 1, true);
                usleep(10'000);
                request.methodLine = "ClearCommandPort";
                tester.executeWaitMultipleData({request}, 1, true);
            }
        });

        // Spam status messages on the control port.
        thread t2([&tester](){
            for (int i = 0; i < 100'000; i++) {
                SData request("Status");
                tester.executeWaitMultipleData({request}, 1, true);
            }
        });

        t1.join();
        t2.join();
    }
} __SupporessCommandPortCrashTest;

