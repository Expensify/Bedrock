#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>

/*
 * Command line args to support:
 * -only            : comma separated list of tests to run.
 * -except          : comma separated list of tests to skip.
 * -dontStartServer : Doesn't start the server, just prints the command that would have been run.
 * -wait            : Waits before running tests, in case you want to connect with the debugger.
 */

void sigclean(int sig) {
    cout << "Got SIGINT, cleaning up." << endl;
    BedrockTester::stopAll();
    cout << "Done." << endl;
    exit(1);
}

int main(int argc, char* argv[]) {
    SData args = SParseCommandLine(argc, argv);
    BedrockTester::globalArgs = args;

    // Catch sigint.
    signal(SIGINT, sigclean);

    // Tests to run (or skip);
    set<string> include;
    set<string> exclude;
    list<string> before;
    list<string> after;
    int threads = 1;

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

    // Perf is excluded unless specified explicitly.
    if (args.isSet("-perf")) {
        include.insert("Perf");
        exclude.erase("Perf");
    } else {
        include.erase("Perf");
        exclude.insert("Perf");
    }

    // Set the defaults for the servers that each BedrockTester will start.
    BedrockTester::defaultDBFile = BedrockTester::getTempFileName();
    cout << "Temp file for this run: " << BedrockTester::defaultDBFile << endl;
    BedrockTester::defaultServerAddr = "127.0.0.1:8989";

    if (args.isSet("-wait")) {
        cout << "Waiting... (type anything to continue)." << endl;
        string temp;
        cin >> temp;
    }

    int retval = 0;
    try {
        retval = tpunit::Tests::run(include, exclude, before, after, threads);
    } catch (...) {
        cout << "Unhandled exception running tests!" << endl;
        retval = 1;
    }

    return retval;
}
