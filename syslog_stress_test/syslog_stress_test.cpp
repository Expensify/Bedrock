#include <atomic>
#include <thread>
#include <iostream>
#include <list>
#include <random>
#include <regex>
#include <string>
#include <syslog.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
using namespace std;

void fake_syslog(int fd, const char* format, ...) {

size_t strftime (char* ptr, size_t maxsize, const char* format, const struct tm* timeptr );
    // Date.
    time_t rawtime;
    struct tm* timeinfo;
    char buffer[80];
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(buffer, 80, "%FT%T ", timeinfo);
    dprintf(fd, "%s", buffer);

    // Args.
    va_list argptr;
    va_start(argptr, format);
    vdprintf(fd, format, argptr);
    va_end(argptr);

    // Endline.
    dprintf(fd, "\n");
}

string make_message(const string& msg) {
    stringstream message;

    // Header.
    message << "<" << 12 << ">";// << 1 << "-" << " " << "-" << " " << "drop-log-tester" << " " << "-" << " " << "-";
    
    // Structured data.
    //message << " ";
    //message << "-";
    
    // Message.
    //message << " ";
    message << msg;

    return message.str();
}

enum TARGET {
    SYSLOG,
    FLATFILE,
    LOGGER
};

int main (int argc, char** argv) {

    // Pick a random ID.
    random_device rd;  //Will be used to obtain a seed for the random number engine
    mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    uniform_int_distribution<> distrib(1, 100'000'000);
    const int randomID = distrib(gen);
 
    // Set our number of threads and number of loglines.
    uint64_t logLineCount = 100'000;
    atomic<uint64_t> currentLogLine(1);
    size_t threadCount = 4;
    TARGET target = SYSLOG;

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
            } else if (name == "target") {
                if (m[2] != "syslog" && m[2] != "filePerThread" && m[2] != "socket") {
                    cout << "Invalid target: " << m[2] << endl;
                } else {
                    if (m[2] == "syslog") {
                        target = SYSLOG;
                    }
                    if (m[2] == "filePerThread") {
                        target = FLATFILE;
                    }
                    if (m[2] == "socket") {
                        target = LOGGER;
                    }
                }
            } else {
                cout << "Unsupported option: " << argv[i] << endl;
                return 1;
            }
        } else {
            cout << "Invalid option: " << argv[i] << endl;
            return 1;
        }
    }

    // Ready...
    cout << "Starting up. Using " << threadCount << " threads for " << logLineCount <<  " log lines." << endl;
    if (target == SYSLOG) {
        openlog(0, 0, 0);
    }

    // Go!
    list<thread> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.emplace_back([i, &currentLogLine, logLineCount, target, randomID]() {
            int logFD = 0;
            static struct sockaddr_un s_addr;	/* AF_UNIX address of local logger */
            int fd = 0;
            if (target == FLATFILE) {
                // open a log file.
                char log_template[] = "/tmp/temp_log_XXXXXX";
                logFD = mkstemp(log_template);
            } else if (target == LOGGER) {
                s_addr.sun_family = AF_UNIX;
                strcpy(s_addr.sun_path, "/run/systemd/journal/syslog");
                fd = socket(AF_UNIX, SOCK_DGRAM, 0);
            }
            if (logFD == -1) {
                cout << "Failed to open log file" << endl;
            }

            while (true) {
                uint64_t logNum = currentLogLine.fetch_add(1);

                // Should we show some progress?
                if (logNum > logLineCount) {
                    break;
                }
                double lastPercent = ((double)(logNum - 1) / (double)logLineCount) * 100;
                double nextPercent = ((double)(logNum) / (double)logLineCount) * 100;
                if ((int)lastPercent != (int)nextPercent) {
                    cout << "Percent complete: " << (int)nextPercent << endl;
                }
                if (target == SYSLOG) {
                    syslog(LOG_WARNING, "logging integer ID: %lu from thread ID: %lu SYSLOG (testID: %i)", logNum, i, randomID);
                } else if (target == LOGGER) {
                    char str[500];
                    snprintf(str, 500, "logging integer ID: %lu from thread ID: %lu SOCKET (testID: %i)", logNum, i, randomID);
                    string message = make_message(str);
                    sendto(fd, message.c_str(), message.size(), 0, (struct sockaddr *)&s_addr, sizeof(struct sockaddr_un));
                } else if (target == FLATFILE) {
                    // write to the log file.
                    dprintf(logFD, "logging integer ID: %lu from thread ID: %lu FLATFILE  (testID: %i)\n", logNum, i, randomID);
                }
            }

            if (target == FLATFILE) {
                close(logFD);
            }
        });
    }

    // Done.
    for (auto& thread : threads) { 
        thread.join();
    }
    cout << "All finished." << endl;
    if (target == SYSLOG) {
        closelog();
    }

    return 0;
}
