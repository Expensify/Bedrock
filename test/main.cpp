#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>

/*
 * Command line args to support:
 * -only            : comma separated list of tests to run.
 * -except          : comma separated list of tests to skip.
 * -dontStartServer : Doesn't start the server, just prints the command that would have been run.
 * -wait            : Waits before running tests, in case you want to connect with the debugger.
 */

// Create temporary file. Returns its name or the empty string on failure.
string getTempFileName() {
    char buffer[24] = "bedrocktest_XXXXXX.db";
    int filedes = mkstemps(buffer, 3);
    close(filedes);
    return buffer;
}

void cleanup() {
    if (BedrockTester::serverPIDs.size()) {
        cout << BedrockTester::serverPIDs.size() << " servers to stop." << endl;
        while (BedrockTester::serverPIDs.size()) {
            BedrockTester::stopServer(*(BedrockTester::serverPIDs.begin()));
        }
    }

    // Delete temp file.
    BedrockTester::deleteFile(BedrockTester::DB_FILE);
}

void sigclean(int sig) {
    cout << "Got SIGINT, cleaning up." << endl;
    cleanup();
    cout << "Done." << endl;
    exit(1);
}

int main(int argc, char* argv[]) {
    SData args = SParseCommandLine(argc, argv);

    // Catch sigint.
    signal(SIGINT, sigclean);

    // Tests to run (or skip);
    set<string> include;
    set<string> exclude;

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

    // Set the defaults for the servers that each BedrockTester will start.
    BedrockTester::DB_FILE = getTempFileName();
    cout << "Temp file for this run: " << BedrockTester::DB_FILE << endl;
    BedrockTester::SERVER_ADDR = "127.0.0.1:8989";

    if (args.isSet("-dontStartServer")) {
        BedrockTester::startServers = false;
        cout << "Not starting server, would have run:" << endl;
        cout << BedrockTester::getCommandLine() << endl;
    }

    if (args.isSet("-wait")) {
        cout << "Waiting... (type anything to continue)." << endl;
        string temp;
        cin >> temp;
    }

    int retval = 0;
    try {
        retval = tpunit::Tests::run(include, exclude);
    } catch (...) {
        cout << "Unhandled exception running tests!" << endl;
        retval = 1;
    }

    cleanup();

    return retval;
}
