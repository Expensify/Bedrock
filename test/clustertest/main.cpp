#include <libstuff/libstuff.h>
#include <test/clustertest/BedrockClusterTester.h>
#include <unistd.h>

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

    if (args.isSet("-duplicateRequests")) {
        // Duplicate every request N times.
        cout << "Setting load testing to: " << SToInt(args["-duplicateRequests"]) << endl;
        BedrockTester::mockRequestMode = SToInt(args["-duplicateRequests"]);
    }

    int retval = 0;
    {
        // Create our cluster.
        list<string> queries = {
            "CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)"
        };

        BedrockClusterTester tester(BedrockClusterTester::THREE_NODE_CLUSTER, queries);

        // Our cluster is up and has responded to status, we can run our tests!
        try {
            retval = tpunit::Tests::run();
        } catch (...) {
            cout << "Unhandled exception running tests!" << endl;
            retval = 1;
        }
    }

    // Tester gets destroyed here. Everything's done.
    return retval;
}
