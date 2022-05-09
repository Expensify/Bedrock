#include <libstuff/SData.h>
#include <test/clustertest/BedrockClusterTester.h>

#include <iostream>
#include <fstream>

struct LogrockTest : tpunit::TestFixture {
    LogrockTest() : tpunit::TestFixture("Logrock", TEST(LogrockTest::test)) { }

    // NOTE: This test relies on two processes (the leader and follower Bedrock nodes) both writing to the same temp
    // file at potentially the same time. It's not impossible that these two writes step on each other and this test
    // fails because the file ends up in a corrupt state. If we see intermittent failures in this test, we may want to
    // add some sort of file locking to the reading/writing from this file.
    void test()
    {
        auto start = STimeNow();

        BedrockClusterTester tester;
        ifstream logfile;
        SData command("logrockingest");

        // We're going to send a command to a follower.
        BedrockTester& brtester = tester.getTester(1);

        // open the file
        //logfile.open ("/vagrant/rafe/2022-04-09T13_00.txt");
        logfile.open ("/vagrant/rafe/some-lines.txt");

         string line;
         int count = 0;
         while (getline(logfile, line)) {
             // increment count
             count++;

             // append line to command body
             command.content += line + "\n";

             // if we got 500 lines, send the command
             if (count % 500 == 0) {
                 // send the command
                 brtester.executeWaitMultipleData({command});

                 // reset the content
                 command.content.clear();
             }
         }

         // close the file
         logfile.close();

         if (command.content != "") {
             // send whatever we have left in the structure
             brtester.executeWaitMultipleData({command});
         }

         cout << "Took " << (STimeNow() - start) / 1'000'000 << " seconds to process " << count << " lines." << endl;
    }
} __LogrockTest;
