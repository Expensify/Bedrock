#include <iostream>
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

/*
 * This is based on the 'test' application in the parent directory to this one, but specifically aims to test the
 * redundancy and replication features of bedrock. As such, the intention is to not shut down servers and clean the
 * database between tests, and so in all likelihood, an early failure will carry over into later failures in
 * subsequent tests. This is expected as we want to verify overall database integrity rather than unit test individual
 * bits of functionality.
 */

void sigclean(int sig) {
    cout << "Got SIGINT, cleaning up." << endl;
    BedrockTester::stopAll();
    cout << "Done." << endl;
    exit(1);
}

// This is a bit of a hack and assumes various things about syslogd
void log() {
    int pid = getpid();
    cout << "Starting log recording with pid: " << pid << endl;
    unlink("log.txt");
    execl("/bin/bash", "/bin/bash", "-c", "tail -f /var/log/syslog | grep --line-buffered bedrock > log.txt", (char *)NULL);
}

int main(int argc, char* argv[]) {
    SData args = SParseCommandLine(argc, argv);

    // Catch sigint.
    signal(SIGINT, sigclean);

    set<string> include;
    set<string> exclude;
    list<string> before;
    list<string> after;
    int threads = 1;
    int repeatCount = 1;

    if (args.isSet("-repeatCount")) {
        repeatCount = max(1, SToInt(args["-repeatCount"]));
    }

    if (args.isSet("-only")) {
        list<string> includeList = SParseList(args["-only"]);
        for (string name : includeList) {
            include.insert(name);
        }
    }
    if (args.isSet("-except")) {
        list<string> excludeList = SParseList(args["-except"]);
        for (string name : excludeList) {
            exclude.insert(name);
        }
    }
    if (args.isSet("-before")) {
        list<string> beforeList = SParseList(args["-before"]);
        for (string name : beforeList) {
            before.push_back(name);
        }
    }
    if (args.isSet("-after")) {
        list<string> afterList = SParseList(args["-after"]);
        for (string name : afterList) {
            after.push_back(name);
        }
    }
    if (args.isSet("-threads")) {
        threads = SToInt(args["-threads"]);
    }

    // Enable HCTree for the tests
    if (args.isSet("-enableHctree")) {
        BedrockTester::ENABLE_HCTREE = true;
        cout << "HCTree enabled" << endl;
    }

    SLogLevel(LOG_INFO);
    if (args.isSet("-v")) {
        BedrockTester::VERBOSE_LOGGING = true;
        SLogLevel(LOG_DEBUG);
    }
    if (args.isSet("-q")) {
        BedrockTester::QUIET_LOGGING = true;
        SLogLevel(LOG_WARNING);
    }

    int retval = 0;
    {
        for (int i = 0; i < repeatCount; i++) {
            try {
                retval = tpunit::Tests::run(include, exclude, before, after, threads);
            } catch (...) {
                cout << "Unhandled exception running tests!" << endl;
                retval = 1;
            }
        }
    }

    SStopSignalThread();

    // Tester gets destroyed here. Everything's done.
    return retval;
}
