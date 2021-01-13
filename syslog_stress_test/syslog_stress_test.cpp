#include <atomic>
#include <thread>
#include <iostream>
#include <list>
#include <regex>
#include <string>
#include <syslog.h>
using namespace std;

int main (int argc, char** argv) {
    
    // Set our number of threads and number of loglines.
    uint64_t logLineCount = 10'000'000;
    atomic<uint64_t> currentLogLine(0);
    size_t threadCount = 192;

    // Parse args.
    for (int i = 1; i < argc; i++) {
        regex re("--(\\w+)=(\\S+)");
        cmatch m;
        if (regex_search(argv[i], m, re)) {
            string name = m[1];
            string val = m[2];
            if (name == "logLineCount") {
                logLineCount = stoull(m[2]);
            } else if (name == "threadCount") {
                threadCount = stoul(m[2]);
            }
        } else {
            cout << "Invalid option: " << argv[i] << endl;
            return 1;
        }
    }

    // Ready...
    cout << "Starting up. Using " << threadCount << " threads for " << logLineCount <<  " log lines." << endl;
    openlog(0, 0, 0);

    // Go!
    list<thread> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.emplace_back([i, &currentLogLine, logLineCount]() {
            while (true) {
                uint64_t logNum = currentLogLine.fetch_add(1);

                // Should we show some progress?
                if (logNum > logLineCount) {
                    return;
                }
                double lastPercent = ((double)(logNum - 1) / (double)logLineCount) * 100;
                double nextPercent = ((double)(logNum) / (double)logLineCount) * 100;
                if ((int)lastPercent != (int)nextPercent) {
                    cout << "Percent complete: " << (int)nextPercent << endl;
                }
                syslog(LOG_WARNING, "logging integer ID: %lu from thread ID: %lu", logNum, i);
            }
        });
    }

    // Done.
    for (auto& thread : threads) { 
        thread.join();
    }
    cout << "All finished." << endl;
    closelog();

    return 0;
}
