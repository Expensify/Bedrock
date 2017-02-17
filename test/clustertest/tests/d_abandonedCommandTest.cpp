#include "../BedrockClusterTester.h"
#include <fstream>

struct d_abandonedCommandTest : tpunit::TestFixture {
    d_abandonedCommandTest()
        : tpunit::TestFixture("d_abandonedCommandTest",
                              TEST(d_abandonedCommandTest::abandon)) { }

    void abandon() {
        int count = 0;
        while(1) {
            ifstream log("log.txt");

            string line;
            while (getline(log, line)) {
                count++;
            }
            if (SContains("TEST_MARK", line)) {
                break;
            }
            sleep(1);
        }
        cout << "Counted " << count << " lines in logfile." << endl;
        cout << "Found test marker." << endl;
    }

} __d_abandonedCommandTest;
